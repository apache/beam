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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/engine"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/jobservices"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/worker"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

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
	envs := j.Pipeline.GetComponents().GetEnvironments()
	wks := map[string]*worker.W{}
	for envID := range envs {
		wk, err := makeWorker(envID, j)
		if err != nil {
			j.Failed(err)
			return
		}
		wks[envID] = wk
	}
	// When this function exits, we cancel the context to clear
	// any related job resources.
	defer func() {
		j.CancelFn(fmt.Errorf("runPipeline returned, cleaning up"))
	}()

	j.SendMsg("running " + j.String())
	j.Running()

	if err := executePipeline(j.RootCtx, wks, j); err != nil && !errors.Is(err, jobservices.ErrCancel) {
		j.Failed(err)
		return
	}

	if errors.Is(context.Cause(j.RootCtx), jobservices.ErrCancel) {
		j.SendMsg("pipeline canceled " + j.String())
		j.Canceled()
		return
	}

	j.SendMsg("pipeline completed " + j.String())

	j.SendMsg("terminating " + j.String())
	j.Done()
}

// makeWorker creates a worker for that environment.
func makeWorker(env string, j *jobservices.Job) (*worker.W, error) {
	wk := worker.New(j.String()+"_"+env, env)

	wk.EnvPb = j.Pipeline.GetComponents().GetEnvironments()[env]
	wk.PipelineOptions = j.PipelineOptions()
	wk.JobKey = j.JobKey()
	wk.ArtifactEndpoint = j.ArtifactEndpoint()

	go wk.Serve()

	if err := runEnvironment(j.RootCtx, j, env, wk); err != nil {
		return nil, fmt.Errorf("failed to start environment %v for job %v: %w", env, j, err)
	}
	// Check for connection succeeding after we've created the environment successfully.
	timeout := 1 * time.Minute
	time.AfterFunc(timeout, func() {
		if wk.Connected() || wk.Stopped() {
			return
		}
		err := fmt.Errorf("prism %v didn't get control connection to %v after %v", wk, wk.Endpoint(), timeout)
		j.Failed(err)
		j.CancelFn(err)
	})
	return wk, nil
}

type transformExecuter interface {
	ExecuteUrns() []string
	ExecuteTransform(stageID, tid string, t *pipepb.PTransform, comps *pipepb.Components, watermark mtime.Time, data [][]byte) *worker.B
}

type processor struct {
	transformExecuters map[string]transformExecuter
}

func executePipeline(ctx context.Context, wks map[string]*worker.W, j *jobservices.Job) error {
	pipeline := j.Pipeline
	comps := proto.Clone(pipeline.GetComponents()).(*pipepb.Components)

	// TODO, configure the preprocessor from pipeline options.
	// Maybe change these returns to a single struct for convenience and further
	// annotation?

	handlers := []any{
		Combine(CombineCharacteristic{EnableLifting: true}),
		ParDo(ParDoCharacteristic{DisableSDF: true}),
		Runner(RunnerCharacteristic{
			SDKFlatten:   false,
			SDKReshuffle: false,
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

	topo := prepro.preProcessGraph(comps, j)
	ts := comps.GetTransforms()

	em := engine.NewElementManager(engine.Config{})

	// TODO move this loop and code into the preprocessor instead.
	stages := map[string]*stage{}
	var impulses []string

	for i, stage := range topo {
		tid := stage.transforms[0]
		t := ts[tid]
		urn := t.GetSpec().GetUrn()
		stage.exe = proc.transformExecuters[urn]

		stage.ID = fmt.Sprintf("stage-%03d", i)
		wk := wks[stage.envID]

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
			winCoder, wDec, wEnc := getWindowValueCoders(comps, col, coders)

			var kd func(io.Reader) []byte
			if kcid, ok := extractKVCoderID(col.GetCoderId(), coders); ok {
				kd = collectionPullDecoder(kcid, coders, comps)
			}
			stage.OutputsToCoders[onlyOut] = engine.PColInfo{
				GlobalID:    onlyOut,
				WindowCoder: winCoder,
				WDec:        wDec,
				WEnc:        wEnc,
				EDec:        ed,
				KeyDec:      kd,
			}

			// There's either 0, 1 or many inputs, but they should be all the same
			// so break after the first one.
			for _, global := range t.GetInputs() {
				col := comps.GetPcollections()[global]
				ed := collectionPullDecoder(col.GetCoderId(), coders, comps)
				winCoder, wDec, wEnc := getWindowValueCoders(comps, col, coders)
				stage.inputInfo = engine.PColInfo{
					GlobalID:    global,
					WindowCoder: winCoder,
					WDec:        wDec,
					WEnc:        wEnc,
					EDec:        ed,
				}
				break
			}

			switch urn {
			case urns.TransformGBK:
				em.AddStage(stage.ID, []string{getOnlyValue(t.GetInputs())}, []string{getOnlyValue(t.GetOutputs())}, nil)
				for _, global := range t.GetInputs() {
					col := comps.GetPcollections()[global]
					ed := collectionPullDecoder(col.GetCoderId(), coders, comps)
					winCoder, wDec, wEnc := getWindowValueCoders(comps, col, coders)

					var kd func(io.Reader) []byte
					if kcid, ok := extractKVCoderID(col.GetCoderId(), coders); ok {
						kd = collectionPullDecoder(kcid, coders, comps)
					}
					stage.inputInfo = engine.PColInfo{
						GlobalID:    global,
						WindowCoder: winCoder,
						WDec:        wDec,
						WEnc:        wEnc,
						EDec:        ed,
						KeyDec:      kd,
					}
				}
				em.StageAggregates(stage.ID)
			case urns.TransformImpulse:
				impulses = append(impulses, stage.ID)
				em.AddStage(stage.ID, nil, []string{getOnlyValue(t.GetOutputs())}, nil)
			case urns.TransformTestStream:
				// Add a synthetic stage that should largely be unused.
				em.AddStage(stage.ID, nil, maps.Values(t.GetOutputs()), nil)
				// Decode the test stream, and convert it to the various events for the ElementManager.
				var pyld pipepb.TestStreamPayload
				if err := proto.Unmarshal(t.GetSpec().GetPayload(), &pyld); err != nil {
					return fmt.Errorf("prism error building stage %v - decoding TestStreamPayload: \n%w", stage.ID, err)
				}

				// Ensure awareness of the coder used for the teststream.
				cID, err := lpUnknownCoders(pyld.GetCoderId(), coders, comps.GetCoders())
				if err != nil {
					panic(err)
				}
				mayLP := func(v []byte) []byte {
					return v
				}
				if cID != pyld.GetCoderId() {
					// The coder needed length prefixing. For simplicity, add a length prefix to each
					// encoded element, since we will be sending a length prefixed coder to consume
					// this anyway. This is simpler than trying to find all the re-written coders after the fact.
					mayLP = func(v []byte) []byte {
						var buf bytes.Buffer
						if err := coder.EncodeVarInt((int64)(len(v)), &buf); err != nil {
							panic(err)
						}
						if _, err := buf.Write(v); err != nil {
							panic(err)
						}
						return buf.Bytes()
					}
				}

				tsb := em.AddTestStream(stage.ID, t.Outputs)
				for _, e := range pyld.GetEvents() {
					switch ev := e.GetEvent().(type) {
					case *pipepb.TestStreamPayload_Event_ElementEvent:
						var elms []engine.TestStreamElement
						for _, e := range ev.ElementEvent.GetElements() {
							elms = append(elms, engine.TestStreamElement{Encoded: mayLP(e.GetEncodedElement()), EventTime: mtime.Time(e.GetTimestamp())})
						}
						tsb.AddElementEvent(ev.ElementEvent.GetTag(), elms)
						ev.ElementEvent.GetTag()
					case *pipepb.TestStreamPayload_Event_WatermarkEvent:
						tsb.AddWatermarkEvent(ev.WatermarkEvent.GetTag(), mtime.Time(ev.WatermarkEvent.GetNewWatermark()))
					case *pipepb.TestStreamPayload_Event_ProcessingTimeEvent:
						if ev.ProcessingTimeEvent.GetAdvanceDuration() == int64(mtime.MaxTimestamp) {
							// TODO: Determine the SDK common formalism for setting processing time to infinity.
							tsb.AddProcessingTimeEvent(time.Duration(mtime.MaxTimestamp))
						} else {
							tsb.AddProcessingTimeEvent(time.Duration(ev.ProcessingTimeEvent.GetAdvanceDuration()) * time.Millisecond)
						}
					default:
						return fmt.Errorf("prism error building stage %v - unknown TestStream event type: %T", stage.ID, ev)
					}
				}

			case urns.TransformFlatten:
				inputs := maps.Values(t.GetInputs())
				sort.Strings(inputs)
				em.AddStage(stage.ID, inputs, []string{getOnlyValue(t.GetOutputs())}, nil)
			}
			stages[stage.ID] = stage
		case wk.Env:
			if err := buildDescriptor(stage, comps, wk, em); err != nil {
				return fmt.Errorf("prism error building stage %v: \n%w", stage.ID, err)
			}
			stages[stage.ID] = stage
			j.Logger.Debug("pipelineBuild", slog.Group("stage", slog.String("ID", stage.ID), slog.String("transformName", t.GetUniqueName())))
			outputs := maps.Keys(stage.OutputsToCoders)
			sort.Strings(outputs)
			em.AddStage(stage.ID, []string{stage.primaryInput}, outputs, stage.sideInputs)
			if stage.stateful {
				em.StageStateful(stage.ID, stage.stateTypeLen)
			}
			if stage.onWindowExpiration.TimerFamily != "" {
				slog.Debug("OnWindowExpiration", slog.String("stage", stage.ID), slog.Any("values", stage.onWindowExpiration))
				em.StageOnWindowExpiration(stage.ID, stage.onWindowExpiration)
			}
			if len(stage.processingTimeTimers) > 0 {
				em.StageProcessingTimeTimers(stage.ID, stage.processingTimeTimers)
			}
		default:
			return fmt.Errorf("unknown environment[%v]", t.GetEnvironmentId())
		}
	}

	// Prime the initial impulses, since we now know what consumes them.
	for _, id := range impulses {
		em.Impulse(id)
	}

	// Use an errgroup to limit max parallelism for the pipeline.
	eg, egctx := errgroup.WithContext(ctx)
	eg.SetLimit(8)

	var instID uint64
	bundles := em.Bundles(egctx, j.CancelFn, func() string {
		return fmt.Sprintf("inst%03d", atomic.AddUint64(&instID, 1))
	})
	for {
		select {
		case <-ctx.Done():
			err := context.Cause(ctx)
			j.Logger.Debug("context canceled", slog.Any("cause", err))
			return err
		case rb, ok := <-bundles:
			if !ok {
				err := eg.Wait()
				j.Logger.Debug("pipeline done!", slog.String("job", j.String()), slog.Any("error", err), slog.Any("topo", topo))
				return err
			}
			eg.Go(func() error {
				s := stages[rb.StageID]
				wk := wks[s.envID]
				if err := s.Execute(ctx, j, wk, comps, em, rb); err != nil {
					// Ensure we clean up on bundle failure
					em.FailBundle(rb)
					return err
				}
				return nil
			})
		}
	}
}

func collectionPullDecoder(coldCId string, coders map[string]*pipepb.Coder, comps *pipepb.Components) func(io.Reader) []byte {
	cID, err := lpUnknownCoders(coldCId, coders, comps.GetCoders())
	if err != nil {
		panic(err)
	}
	return pullDecoder(coders[cID], coders)
}

func extractKVCoderID(coldCId string, coders map[string]*pipepb.Coder) (string, bool) {
	c := coders[coldCId]
	if c.GetSpec().GetUrn() == urns.CoderKV {
		return c.GetComponentCoderIds()[0], true
	}
	return "", false
}

func getWindowValueCoders(comps *pipepb.Components, col *pipepb.PCollection, coders map[string]*pipepb.Coder) (engine.WinCoderType, exec.WindowDecoder, exec.WindowEncoder) {
	ws := comps.GetWindowingStrategies()[col.GetWindowingStrategyId()]
	wcID, err := lpUnknownCoders(ws.GetWindowCoderId(), coders, comps.GetCoders())
	if err != nil {
		panic(err)
	}
	return makeWindowCoders(coders[wcID])
}

func getOnlyPair[K comparable, V any](in map[K]V) (K, V) {
	if len(in) != 1 {
		panic(fmt.Sprintf("expected single value map, had %v - %v", len(in), in))
	}
	for k, v := range in {
		return k, v
	}
	panic("unreachable")
}

func getOnlyValue[K comparable, V any](in map[K]V) V {
	_, v := getOnlyPair(in)
	return v
}
