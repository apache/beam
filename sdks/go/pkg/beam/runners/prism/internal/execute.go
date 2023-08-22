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
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
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
	if len(envs) != 1 {
		j.Failed(fmt.Errorf("unable to execute multi-environment pipelines;\npipeline has environments: %+v", envs))
		return
	}
	env, _ := getOnlyPair(envs)
	wk := worker.New(env) // Cheating by having the worker id match the environment id.
	go wk.Serve()

	// When this function exits, we cancel the context to clear
	// any related job resources.
	defer func() {
		j.CancelFn(nil)
	}()
	go runEnvironment(j.RootCtx, j, env, wk)

	j.SendMsg("running " + j.String())
	j.Running()

	err := executePipeline(j.RootCtx, wk, j)
	if err != nil {
		j.Failed(err)
		return
	}
	j.SendMsg("pipeline completed " + j.String())

	// Stop the worker.
	wk.Stop()

	j.SendMsg("terminating " + j.String())
	j.Done()
}

// TODO move environment handling to the worker package.

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

type transformExecuter interface {
	ExecuteUrns() []string
	ExecuteWith(t *pipepb.PTransform) string
	ExecuteTransform(stageID, tid string, t *pipepb.PTransform, comps *pipepb.Components, watermark mtime.Time, data [][]byte) *worker.B
}

type processor struct {
	transformExecuters map[string]transformExecuter
}

func executePipeline(ctx context.Context, wk *worker.W, j *jobservices.Job) error {
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

	topo := prepro.preProcessGraph(comps)
	ts := comps.GetTransforms()

	em := engine.NewElementManager(engine.Config{})

	// TODO move this loop and code into the preprocessor instead.
	stages := map[string]*stage{}
	var impulses []string
	for _, stage := range topo {
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
			buildDescriptor(stage, comps, wk)
			stages[stage.ID] = stage
			slog.Debug("pipelineBuild", slog.Group("stage", slog.String("ID", stage.ID), slog.String("transformName", t.GetUniqueName())))
			outputs := maps.Keys(stage.OutputsToCoders)
			sort.Strings(outputs)
			em.AddStage(stage.ID, []string{stage.primaryInput}, stage.sides, outputs)
		default:
			err := fmt.Errorf("unknown environment[%v]", t.GetEnvironmentId())
			slog.Error("Execute", err)
			return err
		}
	}

	// Prime the initial impulses, since we now know what consumes them.
	for _, id := range impulses {
		em.Impulse(id)
	}

	// Use a channel to limit max parallelism for the pipeline.
	maxParallelism := make(chan struct{}, 8)
	// Execute stages here
	for rb := range em.Bundles(ctx, wk.NextInst) {
		maxParallelism <- struct{}{}
		go func(rb engine.RunBundle) {
			defer func() { <-maxParallelism }()
			s := stages[rb.StageID]
			s.Execute(ctx, j, wk, comps, em, rb)
		}(rb)
	}
	slog.Info("pipeline done!", slog.String("job", j.String()))
	return nil
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
