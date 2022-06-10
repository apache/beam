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

// Package ptest contains utilities for pipeline unit testing.
package ptest

import (
	"context"
	"flag"
	"os"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners" // common runner flag.

	// ptest uses the direct runner to execute pipelines by default.
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/direct"
)

// TODO(herohde) 7/10/2017: add hooks to verify counters, logs, etc.

// Create creates a pipeline and a PCollection with the given values.
func Create(values []interface{}) (*beam.Pipeline, beam.Scope, beam.PCollection) {
	p := beam.NewPipeline()
	s := p.Root()
	return p, s, beam.Create(s, values...)
}

// CreateList creates a pipeline and a PCollection with the given values.
func CreateList(values interface{}) (*beam.Pipeline, beam.Scope, beam.PCollection) {
	p := beam.NewPipeline()
	s := p.Root()
	return p, s, beam.CreateList(s, values)
}

// Create2 creates a pipeline and 2 PCollections with the given values.
func Create2(a, b []interface{}) (*beam.Pipeline, beam.Scope, beam.PCollection, beam.PCollection) {
	p := beam.NewPipeline()
	s := p.Root()
	return p, s, beam.Create(s, a...), beam.Create(s, b...)
}

// CreateList2 creates a pipeline and 2 PCollections with the given values.
func CreateList2(a, b interface{}) (*beam.Pipeline, beam.Scope, beam.PCollection, beam.PCollection) {
	p := beam.NewPipeline()
	s := p.Root()
	return p, s, beam.CreateList(s, a), beam.CreateList(s, b)
}

// Runner is a flag that sets which runner pipelines under test will use.
//
// The test file must have a TestMain that calls Main or MainWithDefault
// to function.
var (
	Runner        = runners.Runner
	defaultRunner = "direct"
	mainCalled    = false
)

func getRunner() string {
	r := *Runner
	if r == "" {
		r = defaultRunner
	}
	return r
}

// DefaultRunner returns the default runner name for the test file.
func DefaultRunner() string {
	return defaultRunner
}

// MainCalled returns true iff Main or MainRet has been called.
func MainCalled() bool {
	return mainCalled
}

// Run runs a pipeline for testing. The semantics of the pipeline is expected
// to be verified through passert.
func Run(p *beam.Pipeline) error {
	_, err := beam.Run(context.Background(), getRunner(), p)
	return err
}

// RunWithMetrics runs a pipeline for testing with that returns metrics.Results
// in the form of Pipeline Result
func RunWithMetrics(p *beam.Pipeline) (beam.PipelineResult, error) {
	return beam.Run(context.Background(), getRunner(), p)
}

// RunAndValidate runs a pipeline for testing and validates the result, failing
// the test if the pipeline fails.
func RunAndValidate(t *testing.T, p *beam.Pipeline) beam.PipelineResult {
	pr, err := RunWithMetrics(p)
	if err != nil {
		t.Fatalf("Failed to execute job: %v", err)
	}
	return pr
}

// Main is an implementation of testing's TestMain to permit testing
// pipelines on runners other than the direct runner.
//
// To enable this behavior, _ import the desired runner, and set the flag
// accordingly. For example:
//
//	import _ "github.com/apache/beam/sdks/v2/go/pkg/runners/flink"
//
//	func TestMain(m *testing.M) {
//		ptest.Main(m)
//	}
//
func Main(m *testing.M) {
	MainWithDefault(m, "direct")
}

// MainWithDefault is an implementation of testing's TestMain to permit testing
// pipelines on runners other than the direct runner, while setting the default
// runner to use.
func MainWithDefault(m *testing.M, runner string) {
	mainCalled = true
	defaultRunner = runner
	if !flag.Parsed() {
		flag.Parse()
	}
	beam.Init()
	os.Exit(m.Run())
}

// MainRet is equivelant to Main, but returns an exit code to pass to os.Exit().
//
// Example:
//
//	func TestMain(m *testing.M) {
//		os.Exit(ptest.Main(m))
//	}
func MainRet(m *testing.M) int {
	return MainRetWithDefault(m, "direct")
}

// MainRetWithDefault is equivelant to MainWithDefault but returns an exit code
// to pass to os.Exit().
func MainRetWithDefault(m *testing.M, runner string) int {
	mainCalled = true
	defaultRunner = runner
	if !flag.Parsed() {
		flag.Parse()
	}
	beam.Init()
	return m.Run()
}
