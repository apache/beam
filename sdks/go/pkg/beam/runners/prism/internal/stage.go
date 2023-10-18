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
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

// link represents the tuple of a transform, the local id, and the global id for
// that transform's respective input or output. Which it is, is context dependant,
// and not knowable from just the link itself, but can be verified against the transform proto.
type link struct {
	transform, local, global string
}

// stage represents a fused subgraph executed in a single environment.
//
// TODO: Consider ignoring environment boundaries and making fusion
// only consider necessary materialization breaks. The data protocol
// should in principle be able to connect two SDK environments directly
// instead of going through the runner at all, which would be a small
// efficiency gain, in runner memory use.
//
// That would also warrant an execution mode where fusion is taken into
// account, but all serialization boundaries remain since the pcollections
// would continue to get serialized.
type stage struct {
	ID           string
	transforms   []string
	primaryInput string   // PCollection used as the parallel input.
	outputs      []link   // PCollections that must escape this stage.
	sideInputs   []link   // Non-parallel input PCollections and their consumers
	internalCols []string // PCollections that escape. Used for precise coder sending.
	envID        string

	exe              transformExecuter
	inputTransformID string
	inputInfo        engine.PColInfo
	desc             *fnpb.ProcessBundleDescriptor
	sides            []string
	prepareSides     func(b *worker.B, tid string, watermark mtime.Time)

	SinkToPCollection map[string]string
	OutputsToCoders   map[string]engine.PColInfo
}

func (s *stage) Execute(ctx context.Context, j *jobservices.Job, wk *worker.W, ds *worker.DataService, comps *pipepb.Components, em *engine.ElementManager, rb engine.RunBundle) error {
	slog.Debug("Execute: starting bundle", "bundle", rb)

	var b *worker.B
	inputData := em.InputForBundle(rb, s.inputInfo)
	var dataReady <-chan struct{}
	switch s.envID {
	case "": // Runner Transforms
		if len(s.transforms) != 1 {
			panic(fmt.Sprintf("unexpected number of runner transforms, want 1: %+v", s))
		}
		tid := s.transforms[0]
		// Runner transforms are processed immeadiately.
		b = s.exe.ExecuteTransform(s.ID, tid, comps.GetTransforms()[tid], comps, rb.Watermark, inputData)
		b.InstID = rb.BundleID
		slog.Debug("Execute: runner transform", "bundle", rb, slog.String("tid", tid))

		// Do some accounting for the fake bundle.
		b.Resp = make(chan *fnpb.ProcessBundleResponse, 1)
		close(b.Resp) // To avoid blocking downstream, since we don't send on this.
		closed := make(chan struct{})
		close(closed)
		dataReady = closed
	case wk.Env:
		b = &worker.B{
			PBDID:  s.ID,
			InstID: rb.BundleID,

			InputTransformID: s.inputTransformID,

			// TODO Here's where we can split data for processing in multiple bundles.
			InputData: inputData,

			SinkToPCollection: s.SinkToPCollection,
			OutputCount:       len(s.outputs),
		}
		b.Init()

		s.prepareSides(b, s.transforms[0], rb.Watermark)

		slog.Debug("Execute: processing", "bundle", rb)
		defer b.Cleanup(wk)
		dataReady = b.ProcessOn(ctx, wk)
	default:
		err := fmt.Errorf("unknown environment[%v]", s.envID)
		slog.Error("Execute", "error", err)
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
			resp, err := b.Progress(ctx, wk)
			if err != nil {
				slog.Debug("SDK Error from progress, aborting progress", "bundle", rb, "error", err.Error())
				break progress
			}
			index, unknownIDs := j.ContributeTentativeMetrics(resp)
			if len(unknownIDs) > 0 {
				md := wk.MonitoringMetadata(ctx, unknownIDs)
				j.AddMetricShortIDs(md)
			}
			slog.Debug("progress report", "bundle", rb, "index", index)
			// Progress for the bundle hasn't advanced. Try splitting.
			if previousIndex == index && !splitsDone {
				sr, err := b.Split(ctx, wk, 0.5 /* fraction of remainder */, nil /* allowed splits */)
				if err != nil {
					slog.Warn("SDK Error from split, aborting splits", "bundle", rb, "error", err.Error())
					break progress
				}
				if sr.GetChannelSplits() == nil {
					slog.Debug("SDK returned no splits", "bundle", rb)
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

	var resp *fnpb.ProcessBundleResponse
	select {
	case resp = <-b.Resp:
		if b.BundleErr != nil {
			return b.BundleErr
		}
	case <-ctx.Done():
		return context.Cause(ctx)
	}

	// Tally metrics immeadiately so they're available before
	// pipeline termination.
	unknownIDs := j.ContributeFinalMetrics(resp)
	if len(unknownIDs) > 0 {
		md := wk.MonitoringMetadata(ctx, unknownIDs)
		j.AddMetricShortIDs(md)
	}
	// TODO(https://github.com/apache/beam/issues/28543) handle side input data properly.
	ds.Commit(b.OutputData)
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
		slog.Debug("returned empty residual application", "bundle", rb, slog.Int("numResiduals", l), slog.String("pcollection", s.primaryInput))
	}
	em.PersistBundle(rb, s.OutputsToCoders, b.OutputData, s.inputInfo, residualData, minOutputWatermark)
	b.OutputData = engine.TentativeData{} // Clear the data.
	return nil
}

func getSideInputs(t *pipepb.PTransform) (map[string]*pipepb.SideInput, error) {
	if t.GetSpec().GetUrn() != urns.TransformParDo {
		return nil, nil
	}
	// TODO, memoize this, so we don't need to repeatedly unmarshal.
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

// buildDescriptor constructs a ProcessBundleDescriptor for bundles of this stage.
//
// Requirements:
// * The set of inputs to the stage only include one parallel input.
// * The side input pcollections are fully qualified with global pcollection ID, ingesting transform, and local inputID.
// * The outputs are fully qualified with global PCollectionID, producing transform, and local outputID.
//
// It assumes that the side inputs are not sourced from PCollections generated by any transform in this stage.
//
// Because we need the local ids for routing the sources/sinks information.
func buildDescriptor(stg *stage, comps *pipepb.Components, wk *worker.W, ds *worker.DataService) error {
	// Assume stage has an indicated primary input

	coders := map[string]*pipepb.Coder{}
	transforms := map[string]*pipepb.PTransform{}

	for _, tid := range stg.transforms {
		transforms[tid] = comps.GetTransforms()[tid]
	}

	// Start with outputs, since they're simple and uniform.
	sink2Col := map[string]string{}
	col2Coders := map[string]engine.PColInfo{}
	for _, o := range stg.outputs {
		col := comps.GetPcollections()[o.global]
		wOutCid, err := makeWindowedValueCoder(o.global, comps, coders)
		if err != nil {
			return fmt.Errorf("buildDescriptor: failed to handle coder on stage %v for output %+v, pcol %q %v:\n%w", stg.ID, o, o.global, prototext.Format(col), err)
		}
		sinkID := o.transform + "_" + o.local
		ed := collectionPullDecoder(col.GetCoderId(), coders, comps)
		wDec, wEnc := getWindowValueCoders(comps, col, coders)
		sink2Col[sinkID] = o.global
		col2Coders[o.global] = engine.PColInfo{
			GlobalID: o.global,
			WDec:     wDec,
			WEnc:     wEnc,
			EDec:     ed,
		}
		transforms[sinkID] = sinkTransform(sinkID, portFor(wOutCid, wk), o.global)
	}

	// Then lets do Side Inputs, since they are also uniform.
	var sides []string
	var prepareSides []func(b *worker.B, watermark mtime.Time)
	for _, si := range stg.sideInputs {
		col := comps.GetPcollections()[si.global]
		oCID := col.GetCoderId()
		nCID, err := lpUnknownCoders(oCID, coders, comps.GetCoders())
		if err != nil {
			return fmt.Errorf("buildDescriptor: failed to handle coder on stage %v for side input %+v, pcol %q %v:\n%w", stg.ID, si, si.global, prototext.Format(col), err)
		}

		sides = append(sides, si.global)
		if oCID != nCID {
			// Add a synthetic PCollection set with the new coder.
			newGlobal := si.global + "_prismside"
			comps.GetPcollections()[newGlobal] = &pipepb.PCollection{
				DisplayData:         col.GetDisplayData(),
				UniqueName:          col.GetUniqueName(),
				CoderId:             nCID,
				IsBounded:           col.GetIsBounded(),
				WindowingStrategyId: col.WindowingStrategyId,
			}
			// Update side inputs to point to new PCollection with any replaced coders.
			transforms[si.transform].GetInputs()[si.local] = newGlobal
		}
		prepSide, err := handleSideInput(si.transform, si.local, si.global, comps, coders, ds)
		if err != nil {
			slog.Error("buildDescriptor: handleSideInputs", err, slog.String("transformID", si.transform))
			return err
		}
		prepareSides = append(prepareSides, prepSide)
	}

	// Finally, the parallel input, which is it's own special snowflake, that needs a datasource.
	// This id is directly used for the source, but this also copies
	// coders used by side inputs to the coders map for the bundle, so
	// needs to be run for every ID.

	col := comps.GetPcollections()[stg.primaryInput]
	wInCid, err := makeWindowedValueCoder(stg.primaryInput, comps, coders)
	if err != nil {
		return fmt.Errorf("buildDescriptor: failed to handle coder on stage %v for primary input, pcol %q %v:\n%w", stg.ID, stg.primaryInput, prototext.Format(col), err)
	}

	ed := collectionPullDecoder(col.GetCoderId(), coders, comps)
	wDec, wEnc := getWindowValueCoders(comps, col, coders)
	inputInfo := engine.PColInfo{
		GlobalID: stg.primaryInput,
		WDec:     wDec,
		WEnc:     wEnc,
		EDec:     ed,
	}

	stg.inputTransformID = stg.ID + "_source"
	transforms[stg.inputTransformID] = sourceTransform(stg.inputTransformID, portFor(wInCid, wk), stg.primaryInput)

	// Add coders for internal collections.
	for _, pid := range stg.internalCols {
		lpUnknownCoders(comps.GetPcollections()[pid].GetCoderId(), coders, comps.GetCoders())
	}

	reconcileCoders(coders, comps.GetCoders())

	desc := &fnpb.ProcessBundleDescriptor{
		Id:                  stg.ID,
		Transforms:          transforms,
		WindowingStrategies: comps.GetWindowingStrategies(),
		Pcollections:        comps.GetPcollections(),
		Coders:              coders,
		StateApiServiceDescriptor: &pipepb.ApiServiceDescriptor{
			Url: wk.Endpoint(),
		},
	}

	stg.desc = desc
	stg.prepareSides = func(b *worker.B, _ string, watermark mtime.Time) {
		for _, prep := range prepareSides {
			prep(b, watermark)
		}
	}
	stg.sides = sides // List of the global pcollection IDs this stage needs to wait on for side inputs.
	stg.SinkToPCollection = sink2Col
	stg.OutputsToCoders = col2Coders
	stg.inputInfo = inputInfo

	wk.Descriptors[stg.ID] = stg.desc
	return nil
}

// handleSideInput returns a closure that will look up the data for a side input appropriate for the given watermark.
func handleSideInput(tid, local, global string, comps *pipepb.Components, coders map[string]*pipepb.Coder, ds *worker.DataService) (func(b *worker.B, watermark mtime.Time), error) {
	t := comps.GetTransforms()[tid]
	sis, err := getSideInputs(t)
	if err != nil {
		return nil, err
	}

	switch si := sis[local]; si.GetAccessPattern().GetUrn() {
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
		return func(b *worker.B, watermark mtime.Time) {
			data := ds.GetAllData(global)

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
		}, nil

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
		return func(b *worker.B, watermark mtime.Time) {
			// May be of zero length, but that's OK. Side inputs can be empty.
			data := ds.GetAllData(global)
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
		}, nil
	default:
		return nil, fmt.Errorf("local input %v (global %v) uses accesspattern %v", local, global, si.GetAccessPattern().GetUrn())
	}
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
