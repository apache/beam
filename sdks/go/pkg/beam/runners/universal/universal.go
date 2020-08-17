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

// Package universal contains a general-purpose runner that can submit jobs
// to any portable Beam runner.
package universal

import (
	"context"
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/xlangx"

	// Importing to get the side effect of the remote execution hook. See init().
	_ "github.com/apache/beam/sdks/go/pkg/beam/core/runtime/harness/init"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/options/jobopts"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/universal/extworker"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/universal/runnerlib"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/vet"
	"github.com/golang/protobuf/proto"
)

func init() {
	// Note that we also _ import harness/init to setup the remote execution hook.
	beam.RegisterRunner("universal", Execute)
}

// Execute executes the pipeline on a universal beam runner.
func Execute(ctx context.Context, p *beam.Pipeline) error {
	if !beam.Initialized() {
		panic(fmt.Sprint("Beam has not been initialized. Call beam.Init() before pipeline construction."))
	}

	if *jobopts.Strict {
		log.Info(ctx, "Strict mode enabled, applying additional validation.")
		if err := vet.Execute(ctx, p); err != nil {
			return errors.Wrap(err, "strictness check failed")
		}
		log.Info(ctx, "Strict mode validation passed.")
	}

	endpoint, err := jobopts.GetEndpoint()
	if err != nil {
		return err
	}

	edges, _, err := p.Build()
	if err != nil {
		return err
	}
	envUrn := jobopts.GetEnvironmentUrn(ctx)
	getEnvCfg := jobopts.GetEnvironmentConfig

	if jobopts.IsLoopback() {
		// TODO(BEAM-10610): Allow user configuration of this port, rather than kernel selected.
		srv, err := extworker.StartLoopback(ctx, 0)
		if err != nil {
			return err
		}
		defer srv.Stop(ctx)
		getEnvCfg = srv.EnvironmentConfig
	}

	pipeline, err := graphx.Marshal(edges, &graphx.Options{Environment: graphx.CreateEnvironment(
		ctx, envUrn, getEnvCfg)})
	if err != nil {
		return errors.WithContextf(err, "generating model pipeline")
	}

	xlangx.PurgeOutputInput(edges, pipeline)

	xlangx.MergeExpandedWithPipeline(edges, pipeline)
	// os.Exit(1)

	// pipeline, err = pipelinex.Update(pipeline, pipeline.Components)
	fmt.Printf("\n\n%v\n\n", pipeline)

	/*
		// Adding Expanded transforms to their counterparts in the Pipeline
		for id, external := range p.ExpandedTransforms {
			pipeline.Requirements = append(pipeline.Requirements, external.Requirements...)

			// Correct update of transform corresponding to the ExpandedTransform
			// TODO(pskevin): Figure if there is a better way of supporting multiple outputs
			transform := pipeline.Components.Transforms[id]
			existingInput := ""
			newInput := ""
			for _, v := range transform.Outputs {
				existingInput = v
			}
			for _, v := range external.ExpandedTransform.Outputs {
				newInput = v
			}

			for _, t := range pipeline.Components.Transforms {
				for idx, i := range t.Inputs {
					if i == existingInput {
						t.Inputs[idx] = newInput
					}
				}
			}

			// Adding components of the Expanded Transforms to the current Pipeline
			for k, v := range external.Components.Transforms {
				pipeline.Components.Transforms[k] = v
			}
			for k, v := range external.Components.Pcollections {
				pipeline.Components.Pcollections[k] = v
			}
			for k, v := range external.Components.WindowingStrategies {
				pipeline.Components.WindowingStrategies[k] = v
			}
			for k, v := range external.Components.Coders {
				pipeline.Components.Coders[k] = v
			}
			for k, v := range external.Components.Environments {
				// TODO(pskevin): Resolve temporary hack to enable LOOPBACK mode
				if k == "go" {
					continue
				}
				pipeline.Components.Environments[k] = v
			}

			pipeline.Components.Transforms[id] = external.ExpandedTransform
		}
	*/
	log.Info(ctx, proto.MarshalTextString(pipeline))

	opt := &runnerlib.JobOptions{
		Name:         jobopts.GetJobName(),
		Experiments:  jobopts.GetExperiments(),
		Worker:       *jobopts.WorkerBinary,
		RetainDocker: *jobopts.RetainDockerContainers,
	}
	_, err = runnerlib.Execute(ctx, pipeline, endpoint, opt, *jobopts.Async)
	return err
}
