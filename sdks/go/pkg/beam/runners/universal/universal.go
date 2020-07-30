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

	// Importing to get the side effect of the remote execution hook. See init().
	_ "github.com/apache/beam/sdks/go/pkg/beam/core/runtime/harness/init"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/options/jobopts"
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
	pipeline, err := graphx.Marshal(edges, &graphx.Options{Environment: graphx.CreateEnvironment(
		ctx, jobopts.GetEnvironmentUrn(ctx), jobopts.GetEnvironmentConfig)})
	if err != nil {
		return errors.WithContextf(err, "generating model pipeline")
	}
	fmt.Printf("\n\n+++n%v\n+++n\n", pipeline.Components.Transforms)
	// Adding ExpandedTransforms to the pipeline
	for id, external := range p.ExpandedTransforms {
		pipeline.Requirements = append(pipeline.Requirements, external.Requirements...)
		fmt.Printf("\n\n+++n%v\n+++n\n", external.ExpandedTransform)

		// Correct update of transform corresponding to the ExpandedTransform
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
			pipeline.Components.Environments[k] = v
		}

		pipeline.Components.Transforms[id] = external.ExpandedTransform
	}

	// panic("DIE")
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
