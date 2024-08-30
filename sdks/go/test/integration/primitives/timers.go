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

package primitives

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/timers"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/teststream"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/periodic"
)

// Based on https://github.com/apache/beam/blob/master/runners/flink/src/test/java/org/apache/beam/runners/flink/PortableTimersExecutionTest.java

func init() {
	register.DoFn2x0[[]byte, func(string, int)](&inputFn[string, int]{})
	register.DoFn6x0[beam.Window, state.Provider, timers.Provider, string, int, func(kv[string, int])](&eventTimeFn{})
	register.Emitter2[string, int]()
	register.Emitter1[kv[string, int]]()
}

type kv[K, V any] struct {
	Key   K
	Value V
}

func kvfn[K, V any](k K, v V) kv[K, V] {
	return kv[K, V]{k, v}
}

type inputFn[K, V any] struct {
	Inputs []kv[K, V]
}

func (fn *inputFn[K, V]) ProcessElement(_ []byte, emit func(K, V)) {
	for _, in := range fn.Inputs {
		emit(in.Key, in.Value)
	}
}

type eventTimeFn struct {
	Callback timers.EventTime
	MyKey    state.Value[string]

	Offset      int
	TimerOutput int
}

func (fn *eventTimeFn) ProcessElement(w beam.Window, sp state.Provider, tp timers.Provider, key string, value int, emit func(kv[string, int])) {
	fn.Callback.Set(tp, w.MaxTimestamp().ToTime())
	fn.MyKey.Write(sp, key)
	emit(kvfn(key, value+fn.Offset))
}

func (fn *eventTimeFn) OnTimer(ctx context.Context, ts beam.EventTime, sp state.Provider, tp timers.Provider, key string, timer timers.Context, emit func(kv[string, int])) {
	switch timer.Family {
	case fn.Callback.Family:
		switch timer.Tag {
		case "":
			read, ok, err := fn.MyKey.Read(sp)
			if err != nil {
				panic(err)
			}
			if !ok {
				panic("State must be set for key: " + key)
			}
			emit(kvfn(read, fn.TimerOutput))
		default:
			panic("unexpected timer tag: " + timer.Family + " tag:" + timer.Tag + " want: \"\", for key:" + key)
		}
	default:
		if fn.Callback.Family != timer.Family || timer.Tag != "" {
			panic("unexpected timer family: " + timer.Family + " tag:" + timer.Tag + " want: " + fn.Callback.Family + ", for key:" + key)
		}
	}
}

// timersEventTimePipelineBuilder takes in an impulse transform and produces a pipeline construction
// function that validates EventTime timers.
//
// The impulse is provided outside to swap between a bounded impulse, and
// an unbounded one, because the Go SDK uses that to determine if a pipeline
// is "streaming" or not. This matters at least for executions on Dataflow.
//
// The test produces some number of key value elements, with various event time offsets,
// expecting that a single timer event is what fires. The test validates that all the
// input elements, and the timer elements have been emitted.
func timersEventTimePipelineBuilder(makeImp func(s beam.Scope) beam.PCollection) func(s beam.Scope) {
	return func(s beam.Scope) {
		var inputs, wantOutputs []kv[string, int]

		offset := 5000
		timerOutput := 4093

		numKeys := 50
		numDuplicateTimers := 15

		for key := 0; key < numKeys; key++ {
			k := strconv.Itoa(key)
			wantOutputs = append(wantOutputs, kvfn(k, timerOutput))

			for i := 0; i < numDuplicateTimers; i++ {
				inputs = append(inputs, kvfn(k, i))
				wantOutputs = append(wantOutputs, kvfn(k, i+offset))
			}
		}

		imp := makeImp(s)

		keyed := beam.ParDo(s, &inputFn[string, int]{
			Inputs: inputs,
		}, imp)
		times := beam.ParDo(s, &eventTimeFn{
			Offset:      offset,
			TimerOutput: timerOutput,
			Callback:    timers.InEventTime("Callback"),
			MyKey:       state.MakeValueState[string]("MyKey"),
		}, keyed)
		passert.EqualsList(s, times, wantOutputs)
	}
}

// TimersEventTimeBounded validates event time timers in a bounded pipeline.
func TimersEventTimeBounded(s beam.Scope) {
	timersEventTimePipelineBuilder(beam.Impulse)(s)
}

// TimersEventTimeUnbounded validates event time timers in an unbounded pipeline.
func TimersEventTimeUnbounded(s beam.Scope) {
	timersEventTimePipelineBuilder(func(s beam.Scope) beam.PCollection {
		now := time.Now()
		return periodic.Impulse(s, now, now.Add(10*time.Second), 0, false)
	})(s)
}

// Below here are tests for ProcessingTime timers.

func init() {
	register.DoFn2x0[[]byte, func(string, int)](&inputFn[string, int]{})
	register.DoFn5x0[state.Provider, timers.Provider, string, int, func(string, int)](&processingTimeFn{})
}

type processingTimeFn struct {
	Callback  timers.ProcessingTime
	MyValue   state.Value[int]
	Emissions state.Value[int]

	Offset      int
	TimerOutput int
	Cap         int
}

func (fn *processingTimeFn) ProcessElement(sp state.Provider, tp timers.Provider, key string, value int, emit func(string, int)) {
	// Sets a processing time callback to occur.
	fn.Callback.Set(tp, time.Now().Add(9*time.Second))

	// Only write to the state if we haven't done so already.
	// Writing blind would reset the state, and cause duplicated outputs.
	_, ok, err := fn.MyValue.Read(sp)
	if err != nil {
		panic(err)
	}
	if !ok {
		if err := fn.MyValue.Write(sp, 0); err != nil {
			panic(err)
		}
	}
}

func (fn *processingTimeFn) OnTimer(ctx context.Context, ts beam.EventTime, sp state.Provider, tp timers.Provider, key string, timer timers.Context, emit func(string, int)) {
	switch timer.Family {
	case fn.Callback.Family:
		switch timer.Tag {
		case "":
			read, ok, err := fn.MyValue.Read(sp)
			if err != nil {
				panic(err)
			}
			if !ok {
				panic("State must be set for key: " + key)
			}
			emit(key, read)
			if read < fn.Cap-1 {
				if err := fn.MyValue.Write(sp, read+1); err != nil {
					panic(err)
				}
				fn.Callback.Set(tp, time.Now().Add(9*time.Second))
			}
			if num, _, err := fn.Emissions.Read(sp); err != nil {
				panic(err)
			} else if num == fn.Cap {
				panic(fmt.Sprintf("cap reached! This shouldn't be possible. key %v, num: %v, cap %v read %v", key, num, fn.Cap, read))
			} else {
				if err := fn.Emissions.Write(sp, num+1); err != nil {
					panic(err)
				}
			}
		default:
			panic("unexpected timer tag: " + timer.Family + " tag:" + timer.Tag + " want: \"\", for key:" + key)
		}
	default:
		if fn.Callback.Family != timer.Family || timer.Tag != "" {
			panic("unexpected timer family: " + timer.Family + " tag:" + timer.Tag + " want: " + fn.Callback.Family + ", for key:" + key)
		}
	}
}

func regroup(key string, vs func(*int) bool, emit func(kv[string, int])) {
	var v int
	for vs(&v) {
		emit(kvfn(key, v))
	}
}

func init() {
	register.Function3x0(regroup)
}

func timersProcessingTimePipelineBuilder(makeImp func(s beam.Scope) beam.PCollection) func(s beam.Scope) {
	return func(s beam.Scope) {
		var inputs, wantOutputs []kv[string, int]

		offset := 5000
		timerOutput := 4093

		numKeys := 40
		numDuplicateTimers := 15

		for key := 0; key < numKeys; key++ {
			k := strconv.Itoa(key)
			for i := 0; i < numDuplicateTimers; i++ {
				inputs = append(inputs, kvfn(k, i))
				wantOutputs = append(wantOutputs, kvfn(k, i))
			}
		}

		imp := makeImp(s)

		keyed := beam.ParDo(s, &inputFn[string, int]{
			Inputs: inputs,
		}, imp)
		times := beam.ParDo(s, &processingTimeFn{
			Offset:      offset,
			TimerOutput: timerOutput,
			Callback:    timers.InProcessingTime("Callback"),
			MyValue:     state.MakeValueState[int]("MyValue"),
			Cap:         numDuplicateTimers, // Syncs the cycles to the number of duplicate keyed inputs.
		}, keyed)
		// We GroupByKey here so input to passert is blocked until teststream advances time to Infinity.
		gbk := beam.GroupByKey(s, times)
		regrouped := beam.ParDo(s, regroup, gbk)
		passert.EqualsList(s, regrouped, wantOutputs)
	}
}

// TimersProcessingTimeTestStream_Infinity validates processing time timers in a bounded pipeline
// kicked along by TestStream.
func TimersProcessingTimeTestStream_Infinity(s beam.Scope) {
	timersProcessingTimePipelineBuilder(func(s beam.Scope) beam.PCollection {
		c := teststream.NewConfig()
		c.AddElements(123456, []byte{42})
		c.AdvanceWatermarkToInfinity()
		c.AdvanceProcessingTime(int64(mtime.FromDuration(10 * time.Second)))
		c.AdvanceProcessingTime(int64(mtime.FromDuration(10 * time.Second)))
		c.AdvanceProcessingTime(int64(mtime.FromDuration(10 * time.Second)))
		c.AdvanceProcessingTimeToInfinity()
		return teststream.Create(s, c)
	})(s)
}

// TimersProcessingTimeBounded validates processing time timers in a bounded pipeline.
func TimersProcessingTime_Bounded(s beam.Scope) {
	timersProcessingTimePipelineBuilder(beam.Impulse)(s)
}

// TimersProcessingTimeBounded validates processing time timers in an unbounded pipeline.
func TimersProcessingTime_Unbounded(s beam.Scope) {
	timersProcessingTimePipelineBuilder(func(s beam.Scope) beam.PCollection {
		now := time.Now()
		return periodic.Impulse(s, now, now.Add(10*time.Second), 0, false)
	})(s)
}
