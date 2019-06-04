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

	"github.com/apache/beam/sdks/go/pkg/beam"

	// ptest uses the direct runner to execute pipelines by default.
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/direct"
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
	Runner        = flag.String("runner", "", "Pipeline runner.")
	defaultRunner = "direct"
)

// Run runs a pipeline for testing. The semantics of the pipeline is expected
// to be verified through passert.
func Run(p *beam.Pipeline) error {
	if *Runner == "" {
		*Runner = defaultRunner
	}
	return beam.Run(context.Background(), *Runner, p)
}

// Main is an implementation of testing's TestMain to permit testing
// pipelines on runners other than the direct runner.
//
// To enable this behavior, _ import the desired runner, and set the flag accordingly.
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
	defaultRunner = runner
	if !flag.Parsed() {
		flag.Parse()
	}
	beam.Init()
	os.Exit(m.Run())
}
