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
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
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
	pipeline, err := graphx.Marshal(edges, &graphx.Options{Environment: createEnvironment(ctx)})
	if err != nil {
		return errors.WithContextf(err, "generating model pipeline")
	}

	log.Info(ctx, proto.MarshalTextString(pipeline))

	opt := &runnerlib.JobOptions{
		Name:        jobopts.GetJobName(),
		Experiments: jobopts.GetExperiments(),
		Worker:      *jobopts.WorkerBinary,
	}
	_, err = runnerlib.Execute(ctx, pipeline, endpoint, opt, *jobopts.Async)
	return err
}

func createEnvironment(ctx context.Context) pb.Environment {
	var environment pb.Environment
	switch urn := jobopts.GetEnvironmentUrn(ctx); urn {
	case "beam:env:process:v1":
		// TODO Support process based SDK Harness.
		panic(fmt.Sprintf("Unsupported environment %v", urn))
	case "beam:env:docker:v1":
		fallthrough
	default:
		config := jobopts.GetEnvironmentConfig(ctx)
		payload := &pb.DockerPayload{ContainerImage: config}
		serializedPayload, err := proto.Marshal(payload)
		if err != nil {
			panic(fmt.Sprintf(
				"Failed to serialize Environment payload %v for config %v: %v", payload, config, err))
		}
		environment = pb.Environment{
			Urn:     urn,
			Payload: serializedPayload,
		}
	}
	return environment
}
