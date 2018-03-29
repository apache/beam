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
	"github.com/apache/beam/sdks/go/pkg/beam/options/jobopts"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/universal/runnerlib"
)

func init() {
	// Note that we also _ import harness/init to setup the remote execution hook.
	beam.RegisterRunner("universal", Execute)
}

// Execute executes the pipeline on a universal beam runner.
func Execute(ctx context.Context, p *beam.Pipeline) error {
	endpoint, err := jobopts.GetEndpoint()
	if err != nil {
		return err
	}

	edges, _, err := p.Build()
	if err != nil {
		return err
	}
	pipeline, err := graphx.Marshal(edges, &graphx.Options{ContainerImageURL: jobopts.GetContainerImage(ctx)})
	if err != nil {
		return fmt.Errorf("failed to generate model pipeline: %v", err)
	}

	opt := &runnerlib.JobOptions{
		Name:               jobopts.GetJobName(),
		Experiments:        jobopts.GetExperiments(),
		Worker:             *jobopts.WorkerBinary,
		InternalJavaRunner: *jobopts.InternalJavaRunner,
	}
	_, err = runnerlib.Execute(ctx, pipeline, endpoint, opt, *jobopts.Async)
	return err
}
