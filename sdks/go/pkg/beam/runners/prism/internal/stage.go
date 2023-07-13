// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/engine"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/jobservices"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/worker"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slog"
	"google.golang.org/protobuf/proto"
)

// stage represents a fused subgraph.
//
// TODO: do we guarantee that they are all
// the same environment at this point, or
// should that be handled later?
type stage struct {
	ID         string
	transforms []string

	envID            string
	exe              transformExecuter
	outputCount      int
	inputTransformID string
	mainInputPCol    string
	inputInfo        engine.PColInfo
	desc             *fnpb.ProcessBundleDescriptor
	sides            []string
	prepareSides     func(b *worker.B, tid string, watermark mtime.Time)

	SinkToPCollection map[string]string
	OutputsToCoders   map[string]engine.PColInfo
}

func (s *stage) Execute(j *jobservices.Job, wk *worker.W, comps *pipepb.Components, em *engine.ElementManager, rb engine.RunBundle) {
	tid := s.transforms[0]
	slog.Debug("Execute: starting bundle", "bundle", rb, slog.String("tid", tid))

	var b *worker.B
	inputData := em.InputForBundle(rb, s.inputInfo)
	var dataReady <-chan struct{}
	switch s.envID {
	case "": // Runner Transforms
		// Runner transforms are processed immeadiately.
		b = s.exe.ExecuteTransform(tid, comps.GetTransforms()[tid], comps, rb.Watermark, inputData)
		b.InstID = rb.BundleID
		slog.Debug("Execute: runner transform", "bundle", rb, slog.String("tid", tid))

		// Do some accounting for the fake bundle.
		b.Resp = make(chan *fnpb.ProcessBundleResponse, 1)
		close(b.Resp) // To avoid blocking downstream, since we don't send on this.
		closed := make(chan struct{})
		close(closed)
		dataReady = closed
	case wk.ID:
		b = &worker.B{
			PBDID:  s.ID,
			InstID: rb.BundleID,

			InputTransformID: s.inputTransformID,

			// TODO Here's where we can split data for processing in multiple bundles.
			InputData: inputData,

			SinkToPCollection: s.SinkToPCollection,
			OutputCount:       s.outputCount,
		}
		b.Init()

		s.prepareSides(b, s.transforms[0], rb.Watermark)

		slog.Debug("Execute: processing", "bundle", rb)
		defer b.Cleanup(wk)
		dataReady = b.ProcessOn(wk)
	default:
		err := fmt.Errorf("unknown environment[%v]", s.envID)
		slog.Error("Execute", err)
		panic(err)
	}

	// Progress + split loop.
	previousIndex := int64(-2)
	var splitsDone bool
	progTick := time.NewTicker(100 * time.Millisecond)
progress:
	for {
		select {
		case <-dataReady:
			progTick.Stop()
			break progress // exit progress loop on close.
		case <-progTick.C:
			resp := b.Progress(wk)
			index, unknownIDs := j.ContributeTentativeMetrics(resp)
			if len(unknownIDs) > 0 {
				md := wk.MonitoringMetadata(unknownIDs)
				j.AddMetricShortIDs(md)
			}
			slog.Debug("progress report", "bundle", rb, "index", index)
			// Progress for the bundle hasn't advanced. Try splitting.
			if previousIndex == index && !splitsDone {
				sr := b.Split(wk, 0.5 /* fraction of remainder */, nil /* allowed splits */)
				if sr.GetChannelSplits() == nil {
					slog.Warn("split failed", "bundle", rb)
					splitsDone = true
					continue progress
				}
				// TODO sort out rescheduling primary Roots on bundle failure.
				var residualData [][]byte
				for _, rr := range sr.GetResidualRoots() {
					ba := rr.GetApplication()
					residualData = append(residualData, ba.GetElement())
					if len(ba.GetElement()) == 0 {
						slog.LogAttrs(context.TODO(), slog.LevelError, "returned empty residual application", slog.Any("bundle", rb))
						panic("sdk returned empty residual application")
					}
					// TODO what happens to output watermarks on splits?
				}
				if len(sr.GetChannelSplits()) != 1 {
					slog.Warn("received non-single channel split", "bundle", rb)
				}
				cs := sr.GetChannelSplits()[0]
				fr := cs.GetFirstResidualElement()
				// The first residual can be after the end of data, so filter out those cases.
				if len(b.InputData) >= int(fr) {
					b.InputData = b.InputData[:int(fr)]
					em.ReturnResiduals(rb, int(fr), s.inputInfo, residualData)
				}
			} else {
				previousIndex = index
			}
		}
	}
	// Tentative Data is ready, commit it to the main datastore.
	slog.Debug("Execute: commiting data", "bundle", rb, slog.Any("outputsWithData", maps.Keys(b.OutputData.Raw)), slog.Any("outputs", maps.Keys(s.OutputsToCoders)))

	resp, ok := <-b.Resp
	// Bundle has failed, fail the job.
	// TODO add retries & clean up this logic. Channels are closed by the "runner" transforms.
	if !ok && b.Error != "" {
		slog.Error("job failed", "error", b.Error, "bundle", rb, "job", j)
		j.Failed(fmt.Errorf("bundle failed: %v", b.Error))
		return
	}

	// Tally metrics immeadiately so they're available before
	// pipeline termination.
	unknownIDs := j.ContributeFinalMetrics(resp)
	if len(unknownIDs) > 0 {
		md := wk.MonitoringMetadata(unknownIDs)
		j.AddMetricShortIDs(md)
	}
	// TODO handle side input data properly.
	wk.D.Commit(b.OutputData)
	var residualData [][]byte
	var minOutputWatermark map[string]mtime.Time
	for _, rr := range resp.GetResidualRoots() {
		ba := rr.GetApplication()
		residualData = append(residualData, ba.GetElement())
		if len(ba.GetElement()) == 0 {
			slog.LogAttrs(context.TODO(), slog.LevelError, "returned empty residual application", slog.Any("bundle", rb))
			panic("sdk returned empty residual application")
		}
		for col, wm := range ba.GetOutputWatermarks() {
			if minOutputWatermark == nil {
				minOutputWatermark = map[string]mtime.Time{}
			}
			cur, ok := minOutputWatermark[col]
			if !ok {
				cur = mtime.MaxTimestamp
			}
			minOutputWatermark[col] = mtime.Min(mtime.FromTime(wm.AsTime()), cur)
		}
	}
	if l := len(residualData); l > 0 {
		slog.Debug("returned empty residual application", "bundle", rb, slog.Int("numResiduals", l), slog.String("pcollection", s.mainInputPCol))
	}
	em.PersistBundle(rb, s.OutputsToCoders, b.OutputData, s.inputInfo, residualData, minOutputWatermark)
	b.OutputData = engine.TentativeData{} // Clear the data.
}

func getSideInputs(t *pipepb.PTransform) (map[string]*pipepb.SideInput, error) {
	if t.GetSpec().GetUrn() != urns.TransformParDo {
		return nil, nil
	}
	pardo := &pipepb.ParDoPayload{}
	if err := (proto.UnmarshalOptions{}).Unmarshal(t.GetSpec().GetPayload(), pardo); err != nil {
		return nil, fmt.Errorf("unable to decode ParDoPayload")
	}
	return pardo.GetSideInputs(), nil
}

func portFor(wInCid string, wk *worker.W) []byte {
	sourcePort := &fnpb.RemoteGrpcPort{
		CoderId: wInCid,
		ApiServiceDescriptor: &pipepb.ApiServiceDescriptor{
			Url: wk.Endpoint(),
		},
	}
	sourcePortBytes, err := proto.Marshal(sourcePort)
	if err != nil {
		slog.Error("bad port", err, slog.String("endpoint", sourcePort.ApiServiceDescriptor.GetUrl()))
	}
	return sourcePortBytes
}

func buildStage(s *stage, tid string, t *pipepb.PTransform, comps *pipepb.Components, wk *worker.W) {
	s.inputTransformID = tid + "_source"

	coders := map[string]*pipepb.Coder{}
	transforms := map[string]*pipepb.PTransform{
		tid: t, // The Transform to Execute!
	}

	sis, err := getSideInputs(t)
	if err != nil {
		slog.Error("buildStage: getSide Inputs", err, slog.String("transformID", tid))
		panic(err)
	}
	var inputInfo engine.PColInfo
	var sides []string
	for local, global := range t.GetInputs() {
		// This id is directly used for the source, but this also copies
		// coders used by side inputs to the coders map for the bundle, so
		// needs to be run for every ID.
		wInCid := makeWindowedValueCoder(global, comps, coders)
		_, ok := sis[local]
		if ok {
			sides = append(sides, global)
		} else {
			// this is the main input
			transforms[s.inputTransformID] = sourceTransform(s.inputTransformID, portFor(wInCid, wk), global)
			col := comps.GetPcollections()[global]
			ed := collectionPullDecoder(col.GetCoderId(), coders, comps)
			wDec, wEnc := getWindowValueCoders(comps, col, coders)
			inputInfo = engine.PColInfo{
				GlobalID: global,
				WDec:     wDec,
				WEnc:     wEnc,
				EDec:     ed,
			}
		}
		// We need to process all inputs to ensure we have all input coders, so we must continue.
	}

	prepareSides, err := handleSideInputs(t, comps, coders, wk)
	if err != nil {
		slog.Error("buildStage: handleSideInputs", err, slog.String("transformID", tid))
		panic(err)
	}

	// TODO: We need a new logical PCollection to represent the source
	// so we can avoid double counting PCollection metrics later.
	// But this also means replacing the ID for the input in the bundle.
	sink2Col := map[string]string{}
	col2Coders := map[string]engine.PColInfo{}
	for local, global := range t.GetOutputs() {
		wOutCid := makeWindowedValueCoder(global, comps, coders)
		sinkID := tid + "_" + local
		col := comps.GetPcollections()[global]
		ed := collectionPullDecoder(col.GetCoderId(), coders, comps)
		wDec, wEnc := getWindowValueCoders(comps, col, coders)
		sink2Col[sinkID] = global
		col2Coders[global] = engine.PColInfo{
			GlobalID: global,
			WDec:     wDec,
			WEnc:     wEnc,
			EDec:     ed,
		}
		transforms[sinkID] = sinkTransform(sinkID, portFor(wOutCid, wk), global)
	}

	reconcileCoders(coders, comps.GetCoders())

	desc := &fnpb.ProcessBundleDescriptor{
		Id:                  s.ID,
		Transforms:          transforms,
		WindowingStrategies: comps.GetWindowingStrategies(),
		Pcollections:        comps.GetPcollections(),
		Coders:              coders,
		StateApiServiceDescriptor: &pipepb.ApiServiceDescriptor{
			Url: wk.Endpoint(),
		},
	}

	s.desc = desc
	s.outputCount = len(t.Outputs)
	s.prepareSides = prepareSides
	s.sides = sides
	s.SinkToPCollection = sink2Col
	s.OutputsToCoders = col2Coders
	s.mainInputPCol = inputInfo.GlobalID
	s.inputInfo = inputInfo

	wk.Descriptors[s.ID] = s.desc
}

// handleSideInputs ensures appropriate coders are available to the bundle, and prepares a function to stage the data.
func handleSideInputs(t *pipepb.PTransform, comps *pipepb.Components, coders map[string]*pipepb.Coder, wk *worker.W) (func(b *worker.B, tid string, watermark mtime.Time), error) {
	sis, err := getSideInputs(t)
	if err != nil {
		return nil, err
	}
	var prepSides []func(b *worker.B, tid string, watermark mtime.Time)

	// Get WindowedValue Coders for the transform's input and output PCollections.
	for local, global := range t.GetInputs() {
		si, ok := sis[local]
		if !ok {
			continue // This is the main input.
		}

		// this is a side input
		switch si.GetAccessPattern().GetUrn() {
		case urns.SideInputIterable:
			slog.Debug("urnSideInputIterable",
				slog.String("sourceTransform", t.GetUniqueName()),
				slog.String("local", local),
				slog.String("global", global))
			col := comps.GetPcollections()[global]
			ed := collectionPullDecoder(col.GetCoderId(), coders, comps)
			wDec, wEnc := getWindowValueCoders(comps, col, coders)
			// May be of zero length, but that's OK. Side inputs can be empty.

			global, local := global, local
			prepSides = append(prepSides, func(b *worker.B, tid string, watermark mtime.Time) {
				data := wk.D.GetAllData(global)

				if b.IterableSideInputData == nil {
					b.IterableSideInputData = map[string]map[string]map[typex.Window][][]byte{}
				}
				if _, ok := b.IterableSideInputData[tid]; !ok {
					b.IterableSideInputData[tid] = map[string]map[typex.Window][][]byte{}
				}
				b.IterableSideInputData[tid][local] = collateByWindows(data, watermark, wDec, wEnc,
					func(r io.Reader) [][]byte {
						return [][]byte{ed(r)}
					}, func(a, b [][]byte) [][]byte {
						return append(a, b...)
					})
			})

		case urns.SideInputMultiMap:
			slog.Debug("urnSideInputMultiMap",
				slog.String("sourceTransform", t.GetUniqueName()),
				slog.String("local", local),
				slog.String("global", global))
			col := comps.GetPcollections()[global]

			kvc := comps.GetCoders()[col.GetCoderId()]
			if kvc.GetSpec().GetUrn() != urns.CoderKV {
				return nil, fmt.Errorf("multimap side inputs needs KV coder, got %v", kvc.GetSpec().GetUrn())
			}

			kd := collectionPullDecoder(kvc.GetComponentCoderIds()[0], coders, comps)
			vd := collectionPullDecoder(kvc.GetComponentCoderIds()[1], coders, comps)
			wDec, wEnc := getWindowValueCoders(comps, col, coders)

			global, local := global, local
			prepSides = append(prepSides, func(b *worker.B, tid string, watermark mtime.Time) {
				// May be of zero length, but that's OK. Side inputs can be empty.
				data := wk.D.GetAllData(global)
				if b.MultiMapSideInputData == nil {
					b.MultiMapSideInputData = map[string]map[string]map[typex.Window]map[string][][]byte{}
				}
				if _, ok := b.MultiMapSideInputData[tid]; !ok {
					b.MultiMapSideInputData[tid] = map[string]map[typex.Window]map[string][][]byte{}
				}
				b.MultiMapSideInputData[tid][local] = collateByWindows(data, watermark, wDec, wEnc,
					func(r io.Reader) map[string][][]byte {
						kb := kd(r)
						return map[string][][]byte{
							string(kb): {vd(r)},
						}
					}, func(a, b map[string][][]byte) map[string][][]byte {
						if len(a) == 0 {
							return b
						}
						for k, vs := range b {
							a[k] = append(a[k], vs...)
						}
						return a
					})
			})
		default:
			return nil, fmt.Errorf("local input %v (global %v) uses accesspattern %v", local, global, si.GetAccessPattern().GetUrn())
		}
	}
	return func(b *worker.B, tid string, watermark mtime.Time) {
		for _, prep := range prepSides {
			prep(b, tid, watermark)
		}
	}, nil
}

func sourceTransform(parentID string, sourcePortBytes []byte, outPID string) *pipepb.PTransform {
	source := &pipepb.PTransform{
		UniqueName: parentID,
		Spec: &pipepb.FunctionSpec{
			Urn:     urns.TransformSource,
			Payload: sourcePortBytes,
		},
		Outputs: map[string]string{
			"i0": outPID,
		},
	}
	return source
}

func sinkTransform(sinkID string, sinkPortBytes []byte, inPID string) *pipepb.PTransform {
	source := &pipepb.PTransform{
		UniqueName: sinkID,
		Spec: &pipepb.FunctionSpec{
			Urn:     urns.TransformSink,
			Payload: sinkPortBytes,
		},
		Inputs: map[string]string{
			"i0": inPID,
		},
	}
	return source
}

// collateByWindows takes the data and collates them into window keyed maps.
// Uses generics to consolidate the repetitive window loops.
func collateByWindows[T any](data [][]byte, watermark mtime.Time, wDec exec.WindowDecoder, wEnc exec.WindowEncoder, ed func(io.Reader) T, join func(T, T) T) map[typex.Window]T {
	windowed := map[typex.Window]T{}
	for _, datum := range data {
		inBuf := bytes.NewBuffer(datum)
		for {
			ws, _, _, err := exec.DecodeWindowedValueHeader(wDec, inBuf)
			if err == io.EOF {
				break
			}
			// Get the element out, and window them properly.
			e := ed(inBuf)
			for _, w := range ws {
				windowed[w] = join(windowed[w], e)
			}
		}
	}
	return windowed
}
