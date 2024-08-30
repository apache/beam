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

// Package beamx is a convenience package for beam.
package beamx

import (
	"context"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners" // common runner flag.

	// Import the reflection-optimized runtime.
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec/optimized"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/gcs"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"

	// The imports here are for the side effect of runner registration.
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/direct"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dot"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/samza"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/spark"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/universal"
)

var (
	runner        = runners.Runner
	defaultRunner = "prism"
)

func getRunner() string {
	r := *runner
	if r == "" {
		r = defaultRunner
	}
	return r
}

// Run invokes beam.Run with the runner supplied by the flag "runner". It
// defaults to the prism runner, but all beam-distributed runners and textio
// filesystems are implicitly registered.
func Run(ctx context.Context, p *beam.Pipeline) error {
	_, err := beam.Run(ctx, getRunner(), p)
	return err
}

// RunWithMetrics invokes beam.Run with the runner supplied by the
// flag "runner". Returns a beam.PipelineResult objects, which can be
// accessed to query the pipeline's metrics.
func RunWithMetrics(ctx context.Context, p *beam.Pipeline) (beam.PipelineResult, error) {
	return beam.Run(ctx, getRunner(), p)
}
