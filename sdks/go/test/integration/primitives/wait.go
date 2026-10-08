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
	"fmt"
	"math"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/teststream"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/wait"
)

func init() {
	// Package initialization also runs in separately launched SDK workers.
	waitMaxMainTs.Store(math.MinInt64)

	beam.RegisterType(reflect.TypeOf((*waitEvent)(nil)).Elem())
	register.Function1x1(waitPartitionFn)
	register.Function1x1(waitValueFn)
	register.Function2x2(waitCheckSignalFn)
	register.Function2x1(waitRecordMainFn)
	register.Function2x0(waitTimestampFn)
	register.Emitter2[beam.EventTime, waitEvent]()
}

// Prism permits one TestStream per pipeline, so Signal distinguishes its two
// outputs.
type waitEvent struct {
	Signal bool
	V      int
}

// waitStep adds an element or advances the watermark, in milliseconds.
type waitStep struct {
	elem *waitEvent // non-nil for an element step
	ts   int64      // element timestamp, or the new watermark
}

func elemAt(ts int64, signal bool, v int) waitStep {
	return waitStep{elem: &waitEvent{Signal: signal, V: v}, ts: ts}
}

func watermarkTo(ts int64) waitStep {
	return waitStep{ts: ts}
}

func waitPartitionFn(e waitEvent) int {
	if e.Signal {
		return 1
	}
	return 0
}

func waitValueFn(e waitEvent) int {
	return e.V
}

// waitMainAndSignal builds and splits the shared TestStream.
func waitMainAndSignal(s beam.Scope, steps []waitStep) (main, signal beam.PCollection) {
	con := teststream.NewConfig()
	for _, st := range steps {
		var err error
		if st.elem != nil {
			err = con.AddElements(st.ts, *st.elem)
		} else {
			err = con.AdvanceWatermark(st.ts)
		}
		if err != nil {
			panic(err)
		}
	}
	if err := con.AdvanceWatermarkToInfinity(); err != nil {
		panic(err)
	}
	parts := beam.Partition(s, 2, waitPartitionFn, teststream.Create(s, con))
	return parts[0], parts[1]
}

func waitAssertValues(s beam.Scope, out beam.PCollection, want ...int) {
	vals := beam.ParDo(s, waitValueFn, out)
	vals = beam.WindowInto(s, window.NewGlobalWindows(), vals)
	passert.EqualsList(s, vals, want)
}

// runWaitScenario verifies that waiting preserves the main input.
func runWaitScenario(s beam.Scope, steps []waitStep, mainWfn, sigWfn *window.Fn, wantMain ...int) {
	main, signal := waitMainAndSignal(s, steps)
	main = beam.WindowInto(s, mainWfn, main)
	signal = beam.WindowInto(s, sigWfn, signal)
	waitAssertValues(s, wait.On(s, main, signal), wantMain...)
}

func waitThreeWindowSteps() []waitStep {
	return []waitStep{
		elemAt(1_000, false, 1), elemAt(2_000, true, 100),
		watermarkTo(5_000),
		elemAt(16_000, false, 2), elemAt(17_000, true, 200),
		watermarkTo(20_000),
		elemAt(31_000, false, 3), elemAt(31_000, true, 300),
	}
}

// WaitStreamSameFixedWindows waits with main and signal in identical 15s windows.
func WaitStreamSameFixedWindows(s beam.Scope) {
	runWaitScenario(s, waitThreeWindowSteps(),
		window.NewFixedWindows(15*time.Second),
		window.NewFixedWindows(15*time.Second),
		1, 2, 3)
}

// WaitStreamDifferentFixedWindows waits with a 15s main input on a 7s signal.
//
// This and WaitStreamSlidingSignal only check that nothing is lost or
// duplicated when the WindowFns differ. Prism gates side inputs stage-wide by
// watermark, not per mapped window, so the mapping cannot be observed here;
// TestValidateWindowedSideInputs covers the mapping itself.
func WaitStreamDifferentFixedWindows(s beam.Scope) {
	runWaitScenario(s, waitThreeWindowSteps(),
		window.NewFixedWindows(15*time.Second),
		window.NewFixedWindows(7*time.Second),
		1, 2, 3)
}

// WaitStreamSlidingSignal waits on 7s sliding windows emitted every second.
func WaitStreamSlidingSignal(s beam.Scope) {
	runWaitScenario(s, waitThreeWindowSteps(),
		window.NewFixedWindows(15*time.Second),
		window.NewSlidingWindows(1*time.Second, 7*time.Second),
		1, 2, 3)
}

// WaitStreamSomeSignalWindowsEmpty verifies that empty signal windows unblock.
func WaitStreamSomeSignalWindowsEmpty(s beam.Scope) {
	steps := []waitStep{
		elemAt(1_000, false, 1),
		watermarkTo(10_000),
		elemAt(11_000, false, 2), elemAt(12_000, true, 200),
		watermarkTo(20_000),
		elemAt(21_000, false, 3),
	}
	runWaitScenario(s, steps,
		window.NewFixedWindows(10*time.Second),
		window.NewFixedWindows(10*time.Second),
		1, 2, 3)
}

// WaitBoundedGlobalWindow covers bounded global windows on supported runners.
func WaitBoundedGlobalWindow(s beam.Scope) {
	main := beam.Create(s, 1, 2, 3)
	signal := beam.Create(s, "ready")
	passert.Equals(s, wait.On(s, main, signal), 1, 2, 3)
}

func waitTimestampFn(e waitEvent, emit func(beam.EventTime, waitEvent)) {
	emit(mtime.FromMilliseconds(int64(e.V)*1000), e)
}

// waitBoundedEvents timestamps each value at that many seconds.
func waitBoundedEvents(s beam.Scope, vals []int) beam.PCollection {
	events := make([]waitEvent, len(vals))
	for i, v := range vals {
		events[i] = waitEvent{V: v}
	}
	return beam.ParDo(s, waitTimestampFn, beam.CreateList(s, events))
}

// WaitBoundedFixedWindows runs Wait with differently sized fixed windows on
// supported runners.
func WaitBoundedFixedWindows(s beam.Scope) {
	main := beam.WindowInto(s, window.NewFixedWindows(15*time.Second), waitBoundedEvents(s, []int{1, 16, 31}))
	signal := beam.WindowInto(s, window.NewFixedWindows(7*time.Second), waitBoundedEvents(s, []int{2, 17, 31}))
	waitAssertValues(s, wait.On(s, main, signal), 1, 16, 31)
}

// waitMaxMainTs coordinates sequencing assertions when both probe DoFns run in
// the same SDK worker process, as they do in Prism's single worker environment.
var waitMaxMainTs atomic.Int64

// waitCheckSignalFn fails if an older signal arrives after main input was
// released. It runs before wait.On's marker Combine.
func waitCheckSignalFn(ts beam.EventTime, e waitEvent) (waitEvent, error) {
	maxMain := waitMaxMainTs.Load()
	if maxMain != math.MinInt64 && ts.Milliseconds() < maxMain {
		return e, fmt.Errorf("signal element %+v at %v was processed after wait.On released main input up to %v",
			e, ts, mtime.FromMilliseconds(maxMain))
	}
	return e, nil
}

// waitRecordMainFn records the latest released main-input timestamp.
func waitRecordMainFn(ts beam.EventTime, e waitEvent) waitEvent {
	for {
		cur := waitMaxMainTs.Load()
		if ts.Milliseconds() <= cur || waitMaxMainTs.CompareAndSwap(cur, ts.Milliseconds()) {
			return e
		}
	}
}

// WaitStreamSequencing verifies that main@5s is not released before the older
// signal@1s is processed. The allowed-lateness test below separately verifies
// the Never trigger and expiration behavior.
func WaitStreamSequencing(s beam.Scope) {
	waitMaxMainTs.Store(math.MinInt64)

	steps := []waitStep{
		elemAt(5_000, false, 5),
		elemAt(1_000, true, 1),
		watermarkTo(10_000),
		elemAt(15_000, false, 15),
		elemAt(12_000, true, 12),
		watermarkTo(20_000),
	}
	main, signal := waitMainAndSignal(s, steps)
	wfn := window.NewFixedWindows(10 * time.Second)
	main = beam.WindowInto(s, wfn, main)
	signal = beam.WindowInto(s, wfn, beam.ParDo(s, waitCheckSignalFn, signal))

	out := beam.ParDo(s, waitRecordMainFn, wait.On(s, main, signal))
	waitAssertValues(s, out, 5, 15)
}

// waitLatenessSteps adds signal@3s after the watermark reaches 10s. It remains
// valid until the 5s allowed lateness expires.
func waitLatenessSteps() []waitStep {
	return []waitStep{
		elemAt(5_000, false, 5),
		elemAt(2_000, true, 2),
		watermarkTo(10_000),
		elemAt(3_000, true, 3), // late, within the signal's 5s allowed lateness
		watermarkTo(15_000),
	}
}

var waitLatenessFixedWindows = window.NewFixedWindows(10 * time.Second)

func waitAssertLatenessOutput(s beam.Scope, out beam.PCollection) {
	waitAssertValues(s, beam.ParDo(s, waitRecordMainFn, out), 5)
}

// WaitStreamSequencingAllowedLateness verifies that [0,10s) remains blocked
// until 15s when the signal has 5s allowed lateness. An early release causes
// the late signal@3s probe to fail waitCheckSignalFn.
func WaitStreamSequencingAllowedLateness(s beam.Scope) {
	waitMaxMainTs.Store(math.MinInt64)

	main, signal := waitMainAndSignal(s, waitLatenessSteps())
	main = beam.WindowInto(s, waitLatenessFixedWindows, main)
	signal = beam.WindowInto(s, waitLatenessFixedWindows, beam.ParDo(s, waitCheckSignalFn, signal),
		beam.AllowedLateness(5*time.Second))

	waitAssertLatenessOutput(s, wait.On(s, main, signal))
}

// waitStreamSequencingMultipleSignals verifies that every signal blocks,
// regardless of argument order. The signals close at 10s and 15s.
func waitStreamSequencingMultipleSignals(s beam.Scope, strictFirst bool) {
	waitMaxMainTs.Store(math.MinInt64)

	main, signal := waitMainAndSignal(s, waitLatenessSteps())
	main = beam.WindowInto(s, waitLatenessFixedWindows, main)
	checked := beam.ParDo(s, waitCheckSignalFn, signal)
	strict := beam.WindowInto(s.Scope("strict"), waitLatenessFixedWindows, checked)
	lenient := beam.WindowInto(s.Scope("lenient"), waitLatenessFixedWindows, checked,
		beam.AllowedLateness(5*time.Second))

	var out beam.PCollection
	if strictFirst {
		out = wait.On(s, main, strict, lenient)
	} else {
		out = wait.On(s, main, lenient, strict)
	}
	waitAssertLatenessOutput(s, out)
}

// WaitStreamSequencingMultipleSignals waits on a strict signal and then a
// lenient one.
func WaitStreamSequencingMultipleSignals(s beam.Scope) {
	waitStreamSequencingMultipleSignals(s, true)
}

// WaitStreamSequencingMultipleSignalsLenientFirst waits on a lenient signal and
// then a strict one.
func WaitStreamSequencingMultipleSignalsLenientFirst(s beam.Scope) {
	waitStreamSequencingMultipleSignals(s, false)
}
