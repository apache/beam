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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/timers"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/jobopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/engine"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/jobservices"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/universal"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/teststream"
	"github.com/apache/beam/sdks/v2/go/test/integration/primitives"
)

func init() {
	// Not actually being used, but explicitly registering
	// will avoid accidentally using a different runner for
	// the tests if I change things later.
	beam.RegisterRunner("testlocal", execute)
	register.Function1x2[int64, string, int64](keyByConstK)
	register.DoFn7x1[beam.Window, beam.EventTime, state.Provider, timers.Provider, string, int64, func(string), error](&bufferWithTimeoutFn{})
	register.Emitter1[string]()
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
		{pipeline: testStreamStatefulProcessingTimeDirect},
		{pipeline: testStreamStatefulProcessingTimeViaMap},
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

func TestTriggers(t *testing.T) {
	initRunner(t)

	tests := []struct {
		pipeline func(s beam.Scope)
	}{
		{pipeline: primitives.TriggerNever},
		{pipeline: primitives.TriggerAfterAll},
		{pipeline: primitives.TriggerAfterAny},
		{pipeline: primitives.TriggerAfterEach},
		{pipeline: primitives.TriggerAfterEndOfWindow},
		{pipeline: primitives.TriggerRepeat},
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

func keyByConstK(v int64) (string, int64) {
	return "k", v
}

type bufferWithTimeoutFn struct {
	Buf   state.Bag[int64]
	Flush timers.ProcessingTime
}

func (fn *bufferWithTimeoutFn) ProcessElement(_ beam.Window, ts beam.EventTime, sp state.Provider, tp timers.Provider, _ string, val int64, _ func(string)) error {
	existing, ok, err := fn.Buf.Read(sp)
	if err != nil {
		return err
	}
	if !ok || len(existing) == 0 {
		fn.Flush.Set(tp, time.Now().Add(10*time.Second), timers.WithOutputTimestamp(ts.ToTime()))
	}
	return fn.Buf.Add(sp, val)
}

func (fn *bufferWithTimeoutFn) OnTimer(_ context.Context, _ beam.EventTime, sp state.Provider, _ timers.Provider, key string, timer timers.Context, emit func(string)) {
	if timer.Family != fn.Flush.Family {
		panic(fmt.Sprintf("unexpected timer family %q", timer.Family))
	}
	vals, ok, err := fn.Buf.Read(sp)
	if err != nil {
		panic(err)
	}
	if !ok || len(vals) == 0 {
		return
	}
	slices.Sort(vals)
	if err := fn.Buf.Clear(sp); err != nil {
		panic(err)
	}
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = strconv.FormatInt(v, 10)
	}
	emit(fmt.Sprintf("%s:%s", key, strings.Join(parts, ",")))
}

func encodeKVStringInt64(k string, v int64) []byte {
	var buf bytes.Buffer
	if err := coder.EncodeVarInt(int64(len(k)), &buf); err != nil {
		panic(err)
	}
	buf.WriteString(k)
	if err := coder.EncodeVarInt(v, &buf); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func testStreamStatefulProcessingTimeDirect(s beam.Scope) {
	events := []*pipepb.TestStreamPayload_Event{
		{
			Event: &pipepb.TestStreamPayload_Event_ElementEvent{
				ElementEvent: &pipepb.TestStreamPayload_Event_AddElements{
					Elements: []*pipepb.TestStreamPayload_TimestampedElement{
						{EncodedElement: encodeKVStringInt64("k", 1), Timestamp: 200},
						{EncodedElement: encodeKVStringInt64("k", 2), Timestamp: 200},
					},
				},
			},
		},
		{
			Event: &pipepb.TestStreamPayload_Event_ProcessingTimeEvent{
				ProcessingTimeEvent: &pipepb.TestStreamPayload_Event_AdvanceProcessingTime{
					AdvanceDuration: int64(mtime.FromDuration(60 * time.Second)),
				},
			},
		},
		{
			Event: &pipepb.TestStreamPayload_Event_ElementEvent{
				ElementEvent: &pipepb.TestStreamPayload_Event_AddElements{
					Elements: []*pipepb.TestStreamPayload_TimestampedElement{
						{EncodedElement: encodeKVStringInt64("k", 3), Timestamp: 200},
					},
				},
			},
		},
		{
			Event: &pipepb.TestStreamPayload_Event_WatermarkEvent{
				WatermarkEvent: &pipepb.TestStreamPayload_Event_AdvanceWatermark{
					NewWatermark: mtime.MaxTimestamp.Milliseconds(),
				},
			},
		},
	}
	// KV<string, int64> registers c0 (string) and c1 (varint) first, so c2 is the KV coder.
	pyld := protox.MustEncode(&pipepb.TestStreamPayload{
		CoderId: "c2",
		Events:  events,
	})
	kvType := typex.NewKV(typex.New(reflect.TypeFor[string]()), typex.New(reflect.TypeFor[int64]()))
	keyed := beam.External(s, urns.TransformTestStream, pyld, nil, []beam.FullType{kvType}, false)[0]
	out := beam.ParDo(s, &bufferWithTimeoutFn{
		Buf:   state.MakeBagState[int64]("buf"),
		Flush: timers.InProcessingTime("flush"),
	}, keyed)
	passert.Equals(s, out, "k:1,2", "k:3")
}

func testStreamStatefulProcessingTimeViaMap(s beam.Scope) {
	c := teststream.NewConfig()
	if err := c.AddElements(200, int64(1), int64(2)); err != nil {
		panic(err)
	}
	c.AdvanceProcessingTime(int64(mtime.FromDuration(60 * time.Second)))
	if err := c.AddElements(200, int64(3)); err != nil {
		panic(err)
	}
	if err := c.AdvanceWatermarkToInfinity(); err != nil {
		panic(err)
	}
	col := teststream.Create(s, c)
	keyed := beam.ParDo(s, keyByConstK, col)
	out := beam.ParDo(s, &bufferWithTimeoutFn{
		Buf:   state.MakeBagState[int64]("buf"),
		Flush: timers.InProcessingTime("flush"),
	}, keyed)
	passert.Equals(s, out, "k:1,2", "k:3")
}
