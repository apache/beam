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

// Package engine_test ensures coverage of the element manager via pipeline actuation.
package engine_test

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/jobopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/engine"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/jobservices"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/worker"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/universal"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration/primitives"
)

func init() {
	// Not actually being used, but explicitly registering
	// will avoid accidentally using a different runner for
	// the tests if I change things later.
	beam.RegisterRunner("testlocal", execute)
}

func TestMain(m *testing.M) {
	ptest.MainWithDefault(m, "testlocal")
}

func initRunner(t testing.TB) {
	t.Helper()
	if *jobopts.Endpoint == "" {
		s := jobservices.NewServer(0, internal.RunPipeline)
		*jobopts.Endpoint = s.Endpoint()
		go s.Serve()
		t.Cleanup(func() {
			*jobopts.Endpoint = ""
			s.Stop()
		})
	}
	if !jobopts.IsLoopback() {
		*jobopts.EnvironmentType = "loopback"
	}
	// Since we force loopback, avoid cross-compilation.
	f, err := os.CreateTemp("", "dummy")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })
	*jobopts.WorkerBinary = f.Name()
}

func execute(ctx context.Context, p *beam.Pipeline) (beam.PipelineResult, error) {
	return universal.Execute(ctx, p)
}

func executeWithT(ctx context.Context, t testing.TB, p *beam.Pipeline) (beam.PipelineResult, error) {
	t.Helper()
	t.Log("startingTest - ", t.Name())
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	*jobopts.JobName = fmt.Sprintf("%v-%v", strings.ToLower(t.Name()), r1.Intn(1000))
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(lis.Addr().String())
	addr := "localhost:" + port
	g := worker.NewMultiplexW()
	t.Cleanup(g.Stop)
	go g.Serve(lis)
	s := jobservices.NewServer(0, internal.RunPipeline)
	s.WorkerPoolEndpoint = addr
	*jobopts.Endpoint = s.Endpoint()
	go s.Serve()
	t.Cleanup(func() {
		*jobopts.Endpoint = ""
		s.Stop()
	})
	return execute(ctx, p)
}

func initTestName(fn any) string {
	name := reflectx.FunctionName(fn)
	n := strings.LastIndex(name, "/")
	return name[n+1:]
}

// TestStatefulStages validates that stateful transform execution is correct in
// four different modes for producing bundles:
//
//   - Greedily batching all ready keys and elements.
//   - All elements for a single key.
//   - Only one element for each available key.
//   - Only one element.
//
// Executing these pipeline here ensures their coverage is reflected in the
// engine package.
func TestStatefulStages(t *testing.T) {
	initRunner(t)

	tests := []struct {
		pipeline func(s beam.Scope)
	}{
		{pipeline: primitives.BagStateParDo},
		{pipeline: primitives.BagStateParDoClear},
		{pipeline: primitives.CombiningStateParDo},
		{pipeline: primitives.ValueStateParDo},
		{pipeline: primitives.ValueStateParDoClear},
		{pipeline: primitives.ValueStateParDoWindowed},
		{pipeline: primitives.MapStateParDo},
		{pipeline: primitives.MapStateParDoClear},
		{pipeline: primitives.SetStateParDo},
		{pipeline: primitives.SetStateParDoClear},
		{pipeline: primitives.TimersEventTimeBounded},
		{pipeline: primitives.TimersEventTimeUnbounded},
	}

	configs := []struct {
		name                              string
		OneElementPerKey, OneKeyPerBundle bool
	}{
		{"Greedy", false, false},
		{"AllElementsPerKey", false, true},
		{"OneElementPerKey", true, false},
		{"OneElementPerBundle", true, true},
	}
	for _, config := range configs {
		for _, test := range tests {
			t.Run(initTestName(test.pipeline)+"_"+config.name, func(t *testing.T) {
				t.Cleanup(func() {
					engine.OneElementPerKey = false
					engine.OneKeyPerBundle = false
				})
				engine.OneElementPerKey = config.OneElementPerKey
				engine.OneKeyPerBundle = config.OneKeyPerBundle
				p, s := beam.NewPipelineWithRoot()
				test.pipeline(s)
				_, err := executeWithT(context.Background(), t, p)
				if err != nil {
					t.Fatalf("pipeline failed, but feature should be implemented in Prism: %v", err)
				}
			})
		}
	}
}

func TestElementManagerCoverage(t *testing.T) {
	initRunner(t)

	tests := []struct {
		pipeline func(s beam.Scope)
	}{
		{pipeline: primitives.Checkpoints}, // (Doesn't run long enough to split.)
		{pipeline: primitives.WindowSums_Lifted},
	}

	for _, test := range tests {
		t.Run(initTestName(test.pipeline), func(t *testing.T) {
			p, s := beam.NewPipelineWithRoot()
			test.pipeline(s)
			_, err := executeWithT(context.Background(), t, p)
			if err != nil {
				t.Fatalf("pipeline failed, but feature should be implemented in Prism: %v", err)
			}
		})
	}
}

func TestTestStream(t *testing.T) {
	initRunner(t)

	tests := []struct {
		pipeline func(s beam.Scope)
	}{
		{pipeline: primitives.TestStreamBoolSequence},
		{pipeline: primitives.TestStreamByteSliceSequence},
		{pipeline: primitives.TestStreamFloat64Sequence},
		{pipeline: primitives.TestStreamInt64Sequence},
		{pipeline: primitives.TestStreamInt16Sequence},
		{pipeline: primitives.TestStreamStrings},
		{pipeline: primitives.TestStreamTwoBoolSequences},
		{pipeline: primitives.TestStreamTwoFloat64Sequences},
		{pipeline: primitives.TestStreamTwoInt64Sequences},
		{pipeline: primitives.TestStreamTwoUserTypeSequences},

		{pipeline: primitives.TestStreamSimple},
		{pipeline: primitives.TestStreamSimple_InfinityDefault},
		{pipeline: primitives.TestStreamToGBK},
		{pipeline: primitives.TestStreamTimersEventTime},
	}

	configs := []struct {
		name                              string
		OneElementPerKey, OneKeyPerBundle bool
	}{
		{"Greedy", false, false},
		{"AllElementsPerKey", false, true},
		{"OneElementPerKey", true, false},
		{"OneElementPerBundle", true, true},
	}
	for _, config := range configs {
		for _, test := range tests {
			t.Run(initTestName(test.pipeline)+"_"+config.name, func(t *testing.T) {
				t.Cleanup(func() {
					engine.OneElementPerKey = false
					engine.OneKeyPerBundle = false
				})
				engine.OneElementPerKey = config.OneElementPerKey
				engine.OneKeyPerBundle = config.OneKeyPerBundle
				p, s := beam.NewPipelineWithRoot()
				test.pipeline(s)
				_, err := executeWithT(context.Background(), t, p)
				if err != nil {
					t.Fatalf("pipeline failed, but feature should be implemented in Prism: %v", err)
				}
			})
		}
	}
}

// TestProcessingTime is the suite for validating behaviors around ProcessingTime.
// Separate from the TestStream, Timers, and Triggers tests due to the unique nature
// of the time domain.
func TestProcessingTime(t *testing.T) {
	initRunner(t)

	tests := []struct {
		pipeline func(s beam.Scope)
	}{
		{pipeline: primitives.TimersProcessingTimeTestStream_Infinity},
		{pipeline: primitives.TimersProcessingTime_Bounded},
		{pipeline: primitives.TimersProcessingTime_Unbounded},
	}

	configs := []struct {
		name                              string
		OneElementPerKey, OneKeyPerBundle bool
	}{
		{"Greedy", false, false},
		{"AllElementsPerKey", false, true},
		{"OneElementPerKey", true, false},
		// {"OneElementPerBundle", true, true}, // Reveals flaky behavior
	}
	for _, config := range configs {
		for _, test := range tests {
			t.Run(initTestName(test.pipeline)+"_"+config.name, func(t *testing.T) {
				t.Cleanup(func() {
					engine.OneElementPerKey = false
					engine.OneKeyPerBundle = false
				})
				engine.OneElementPerKey = config.OneElementPerKey
				engine.OneKeyPerBundle = config.OneKeyPerBundle
				p, s := beam.NewPipelineWithRoot()
				test.pipeline(s)
				_, err := executeWithT(context.Background(), t, p)
				if err != nil {
					t.Fatalf("pipeline failed, but feature should be implemented in Prism: %v", err)
				}
			})
		}
	}
}
