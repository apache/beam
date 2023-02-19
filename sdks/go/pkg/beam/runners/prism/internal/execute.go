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
	"sort"

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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func executePipeline(ctx context.Context, wk *worker.W, j *jobservices.Job) {
	pipeline := j.Pipeline
	comps := proto.Clone(pipeline.GetComponents()).(*pipepb.Components)

	// TODO, configure the preprocessor from pipeline options.
	// Maybe change these returns to a single struct for convenience and further
	// annotation?

	handlers := []any{
		Combine(CombineCharacteristic{EnableLifting: true}),
		ParDo(ParDoCharacteristic{DisableSDF: true}),
		Runner(RunnerCharacteristic{
			SDKFlatten: false,
		}),
	}

	proc := processor{
		transformExecuters: map[string]transformExecuter{},
	}

	var preppers []transformPreparer
	for _, h := range handlers {
		if th, ok := h.(transformPreparer); ok {
			preppers = append(preppers, th)
		}
		if th, ok := h.(transformExecuter); ok {
			for _, urn := range th.ExecuteUrns() {
				proc.transformExecuters[urn] = th
			}
		}
	}

	prepro := newPreprocessor(preppers)

	topo := prepro.preProcessGraph(comps)
	ts := comps.GetTransforms()

	em := engine.NewElementManager(engine.Config{})

	// This is where the Batch -> Streaming tension exists.
	// We don't *pre* do this, and we need a different mechanism
	// to sort out processing order.
	stages := map[string]*stage{}
	var impulses []string
	for i, stage := range topo {
		if len(stage.transforms) != 1 {
			panic(fmt.Sprintf("unsupported stage[%d]: contains multiple transforms: %v; TODO: implement fusion", i, stage.transforms))
		}
		tid := stage.transforms[0]
		t := ts[tid]
		urn := t.GetSpec().GetUrn()
		stage.exe = proc.transformExecuters[urn]

		// Stopgap until everythinng's moved to handlers.
		stage.envID = t.GetEnvironmentId()
		if stage.exe != nil {
			stage.envID = stage.exe.ExecuteWith(t)
		}
		stage.ID = wk.NextStage()

		switch stage.envID {
		case "": // Runner Transforms

			var onlyOut string
			for _, out := range t.GetOutputs() {
				onlyOut = out
			}
			stage.OutputsToCoders = map[string]engine.PColInfo{}
			coders := map[string]*pipepb.Coder{}
			makeWindowedValueCoder(onlyOut, comps, coders)

			col := comps.GetPcollections()[onlyOut]
			ed := collectionPullDecoder(col.GetCoderId(), coders, comps)
			wDec, wEnc := getWindowValueCoders(comps, col, coders)

			stage.OutputsToCoders[onlyOut] = engine.PColInfo{
				GlobalID: onlyOut,
				WDec:     wDec,
				WEnc:     wEnc,
				EDec:     ed,
			}

			// There's either 0, 1 or many inputs, but they should be all the same
			// so break after the first one.
			for _, global := range t.GetInputs() {
				col := comps.GetPcollections()[global]
				ed := collectionPullDecoder(col.GetCoderId(), coders, comps)
				wDec, wEnc := getWindowValueCoders(comps, col, coders)
				stage.inputInfo = engine.PColInfo{
					GlobalID: global,
					WDec:     wDec,
					WEnc:     wEnc,
					EDec:     ed,
				}
				break
			}

			switch urn {
			case urns.TransformGBK:
				em.AddStage(stage.ID, []string{getOnlyValue(t.GetInputs())}, nil, []string{getOnlyValue(t.GetOutputs())})
				for _, global := range t.GetInputs() {
					col := comps.GetPcollections()[global]
					ed := collectionPullDecoder(col.GetCoderId(), coders, comps)
					wDec, wEnc := getWindowValueCoders(comps, col, coders)
					stage.inputInfo = engine.PColInfo{
						GlobalID: global,
						WDec:     wDec,
						WEnc:     wEnc,
						EDec:     ed,
					}
				}
				em.StageAggregates(stage.ID)
			case urns.TransformImpulse:
				impulses = append(impulses, stage.ID)
				em.AddStage(stage.ID, nil, nil, []string{getOnlyValue(t.GetOutputs())})
			case urns.TransformFlatten:
				inputs := maps.Values(t.GetInputs())
				sort.Strings(inputs)
				em.AddStage(stage.ID, inputs, nil, []string{getOnlyValue(t.GetOutputs())})
			}
			stages[stage.ID] = stage
			wk.Descriptors[stage.ID] = stage.desc
		case wk.ID:
			// Great! this is for this environment. // Broken abstraction.
			buildStage(stage, tid, t, comps, wk)
			stages[stage.ID] = stage
			slog.Debug("pipelineBuild", slog.Group("stage", slog.String("ID", stage.ID), slog.String("transformName", t.GetUniqueName())))
			outputs := maps.Keys(stage.OutputsToCoders)
			sort.Strings(outputs)
			em.AddStage(stage.ID, []string{stage.mainInputPCol}, stage.sides, outputs)
		default:
			err := fmt.Errorf("unknown environment[%v]", t.GetEnvironmentId())
			slog.Error("Execute", err)
			panic(err)
		}
	}

	// Prime the initial impulses, since we now know what consumes them.
	for _, id := range impulses {
		em.Impulse(id)
	}

	// Execute stages here
	for rb := range em.Bundles(ctx, wk.NextInst) {
		s := stages[rb.StageID]
		s.Execute(j, wk, comps, em, rb)
	}
	slog.Info("pipeline done!", slog.String("job", j.String()))
}

func getOnlyValue[K comparable, V any](in map[K]V) V {
	if len(in) != 1 {
		panic(fmt.Sprintf("expected single value map, had %v", len(in)))
	}
	for _, v := range in {
		return v
	}
	panic("unreachable")
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

func collectionPullDecoder(coldCId string, coders map[string]*pipepb.Coder, comps *pipepb.Components) func(io.Reader) []byte {
	cID := lpUnknownCoders(coldCId, coders, comps.GetCoders())
	return pullDecoder(coders[cID], coders)
}

func getWindowValueCoders(comps *pipepb.Components, col *pipepb.PCollection, coders map[string]*pipepb.Coder) (exec.WindowDecoder, exec.WindowEncoder) {
	ws := comps.GetWindowingStrategies()[col.GetWindowingStrategyId()]
	wcID := lpUnknownCoders(ws.GetWindowCoderId(), coders, comps.GetCoders())
	return makeWindowCoders(coders[wcID])
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

type transformExecuter interface {
	ExecuteUrns() []string
	ExecuteWith(t *pipepb.PTransform) string
	ExecuteTransform(tid string, t *pipepb.PTransform, comps *pipepb.Components, watermark mtime.Time, data [][]byte) *worker.B
}

type processor struct {
	transformExecuters map[string]transformExecuter
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
				// if w.MaxTimestamp() > watermark {
				// 	var t T
				// 	slog.Debug(fmt.Sprintf("collateByWindows[%T]: window not yet closed, skipping %v > %v", t, w.MaxTimestamp(), watermark))
				// 	continue
				// }
				windowed[w] = join(windowed[w], e)
			}
		}
	}
	return windowed
}

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
	var send bool
	inputData := em.InputForBundle(rb, s.inputInfo)
	switch s.envID {
	case "": // Runner Transforms
		// Runner transforms are processed immeadiately.
		b = s.exe.ExecuteTransform(tid, comps.GetTransforms()[tid], comps, rb.Watermark, inputData)
		b.InstID = rb.BundleID
		slog.Debug("Execute: runner transform", "bundle", rb, slog.String("tid", tid))
	case wk.ID:
		send = true
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
	default:
		err := fmt.Errorf("unknown environment[%v]", s.envID)
		slog.Error("Execute", err)
		panic(err)
	}

	if send {
		slog.Debug("Execute: processing", "bundle", rb)
		b.ProcessOn(wk) // Blocks until finished.
	}
	// Tentative Data is ready, commit it to the main datastore.
	slog.Debug("Execute: commiting data", "bundle", rb, slog.Any("outputsWithData", maps.Keys(b.OutputData.Raw)), slog.Any("outputs", maps.Keys(s.OutputsToCoders)))

	resp := &fnpb.ProcessBundleResponse{}
	if send {
		resp = <-b.Resp
		// Tally metrics immeadiately so they're available before
		// pipeline termination.
		j.ContributeMetrics(resp)
	}
	// TODO handle side input data properly.
	wk.D.Commit(b.OutputData)
	var residualData [][]byte
	var minOutputWatermark map[string]mtime.Time
	for _, rr := range resp.GetResidualRoots() {
		ba := rr.GetApplication()
		residualData = append(residualData, ba.GetElement())
		if len(ba.GetElement()) == 0 {
			slog.Log(slog.LevelError, "returned empty residual application", "bundle", rb)
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

// RunPipeline starts the main thread fo executing this job.
// It's analoguous to the manager side process for a distributed pipeline.
// It will begin "workers"
func RunPipeline(j *jobservices.Job) {
	j.SendMsg("starting " + j.String())
	j.Start()

	// In a "proper" runner, we'd iterate through all the
	// environments, and start up docker containers, but
	// here, we only want and need the go one, operating
	// in loopback mode.
	env := "go"
	wk := worker.New(env) // Cheating by having the worker id match the environment id.
	go wk.Serve()

	// When this function exits, we
	defer func() {
		j.CancelFn()
	}()
	go runEnvironment(j.RootCtx, j, env, wk)

	j.SendMsg("running " + j.String())
	j.Running()

	executePipeline(j.RootCtx, wk, j)
	j.SendMsg("pipeline completed " + j.String())

	// Stop the worker.
	wk.Stop()

	j.SendMsg("terminating " + j.String())
	j.Done()
}

func runEnvironment(ctx context.Context, j *jobservices.Job, env string, wk *worker.W) {
	// TODO fix broken abstraction.
	// We're starting a worker pool here, because that's the loopback environment.
	// It's sort of a mess, largely because of loopback, which has
	// a different flow from a provisioned docker container.
	e := j.Pipeline.GetComponents().GetEnvironments()[env]
	switch e.GetUrn() {
	case urns.EnvExternal:
		ep := &pipepb.ExternalPayload{}
		if err := (proto.UnmarshalOptions{}).Unmarshal(e.GetPayload(), ep); err != nil {
			slog.Error("unmarshing environment payload", err, slog.String("envID", wk.ID))
		}
		externalEnvironment(ctx, ep, wk)
		slog.Info("environment stopped", slog.String("envID", wk.String()), slog.String("job", j.String()))
	default:
		panic(fmt.Sprintf("environment %v with urn %v unimplemented", env, e.GetUrn()))
	}
}

func externalEnvironment(ctx context.Context, ep *pipepb.ExternalPayload, wk *worker.W) {
	conn, err := grpc.Dial(ep.GetEndpoint().GetUrl(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(fmt.Sprintf("unable to dial sdk worker %v: %v", ep.GetEndpoint().GetUrl(), err))
	}
	defer conn.Close()
	pool := fnpb.NewBeamFnExternalWorkerPoolClient(conn)

	endpoint := &pipepb.ApiServiceDescriptor{
		Url: wk.Endpoint(),
	}

	pool.StartWorker(ctx, &fnpb.StartWorkerRequest{
		WorkerId:          wk.ID,
		ControlEndpoint:   endpoint,
		LoggingEndpoint:   endpoint,
		ArtifactEndpoint:  endpoint,
		ProvisionEndpoint: endpoint,
		Params:            nil,
	})

	// Job processing happens here, but orchestrated by other goroutines
	// This goroutine blocks until the context is cancelled, signalling
	// that the pool runner should stop the worker.
	<-ctx.Done()

	// Previous context cancelled so we need a new one
	// for this request.
	pool.StopWorker(context.Background(), &fnpb.StopWorkerRequest{
		WorkerId: wk.ID,
	})
}
