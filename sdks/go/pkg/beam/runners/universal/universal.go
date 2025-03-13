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

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/xlangx"

	// Importing to get the side effect of the remote execution hook. See init().
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/harness/init"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/jobopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/universal/extworker"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/universal/runnerlib"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/vet"
)

func init() {
	// Note that we also _ import harness/init to setup the remote execution hook.
	beam.RegisterRunner("universal", Execute)
	beam.RegisterRunner("PortableRunner", Execute)
	beam.RegisterRunner("portable", Execute)
}

// Execute executes the pipeline on a universal beam runner.
func Execute(ctx context.Context, p *beam.Pipeline) (beam.PipelineResult, error) {
	if !beam.Initialized() {
		panic("Beam has not been initialized. Call beam.Init() before pipeline construction.")
	}

	if *jobopts.Strict {
		log.Info(ctx, "Strict mode enabled, applying additional validation.")
		if _, err := vet.Execute(ctx, p); err != nil {
			return nil, errors.Wrap(err, "strictness check failed")
		}
		log.Info(ctx, "Strict mode validation passed.")
	}

	endpoint, err := jobopts.GetEndpoint()
	if err != nil {
		return nil, err
	}

	edges, _, err := p.Build()
	if err != nil {
		return nil, err
	}
	envUrn := jobopts.GetEnvironmentUrn(ctx)
	getEnvCfg := jobopts.GetEnvironmentConfig

	if jobopts.IsLoopback() {
		// TODO(BEAM-10610): Allow user configuration of this port, rather than kernel selected.
		srv, err := extworker.StartLoopback(ctx, 0)
		if err != nil {
			return nil, err
		}
		defer srv.Stop(ctx)
		getEnvCfg = srv.EnvironmentConfig
	}

	// Fetch all dependencies for cross-language transforms
	xlangx.ResolveArtifacts(ctx, edges, nil)

	environment, err := graphx.CreateEnvironment(ctx, envUrn, getEnvCfg)
	if err != nil {
		return nil, errors.WithContextf(err, "generating model pipeline")
	}
	pipeline, err := graphx.Marshal(edges, &graphx.Options{
		Environment:           environment,
		PipelineResourceHints: jobopts.GetPipelineResourceHints(),
	})
	if err != nil {
		return nil, errors.WithContextf(err, "generating model pipeline")
	}

	log.Info(ctx, pipeline.String())

	opt := &runnerlib.JobOptions{
		Name:         jobopts.GetJobName(),
		Experiments:  jobopts.GetExperiments(),
		Worker:       *jobopts.WorkerBinary,
		RetainDocker: *jobopts.RetainDockerContainers,
		Parallelism:  *jobopts.Parallelism,
		Loopback:     jobopts.IsLoopback(),
	}
	return runnerlib.Execute(ctx, pipeline, endpoint, opt, *jobopts.Async)
}
