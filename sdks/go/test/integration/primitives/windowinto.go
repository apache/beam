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
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window/trigger"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/teststream"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
)

func init() {
	register.Function4x2(sumPerKey)
	register.Function3x0(sumSideInputs)
	register.DoFn2x0[[]byte, func(beam.EventTime, string, int)](&createTimestampedData{})

	register.Emitter3[beam.EventTime, string, int]()
	register.Emitter1[int]()
	register.Iter1[int]()
}

// createTimestampedData produces data timestamped with the ordinal.
type createTimestampedData struct {
	Data []int
}

func (f *createTimestampedData) ProcessElement(_ []byte, emit func(beam.EventTime, string, int)) {
	for i, v := range f.Data {
		timestamp := mtime.FromMilliseconds(int64((i + 1) * 1000)).Subtract(10 * time.Millisecond)
		emit(timestamp, "magic", v)
	}
}

// WindowSums produces a pipeline that generates the numbers of a 3x3 magic square, and
// configures the pipeline so that PCollection. Sum is a closure to handle summing data over the window, in a few conditions.
func WindowSums(s beam.Scope, sumPerKey func(beam.Scope, beam.PCollection) beam.PCollection) {
	timestampedData := beam.ParDo(s, &createTimestampedData{Data: []int{4, 9, 2, 3, 5, 7, 8, 1, 6}}, beam.Impulse(s))

	windowSize := 3 * time.Second

	validate := func(s beam.Scope, wfn *window.Fn, in beam.PCollection, expected ...any) {
		// Window the data.
		windowed := beam.WindowInto(s, wfn, in)
		// Perform the appropriate sum operation.
		sums := sumPerKey(s, windowed)
		// Drop back to Global windows, and drop the key otherwise passert.Equals doesn't work.
		sums = beam.WindowInto(s, window.NewGlobalWindows(), sums)
		sums = beam.DropKey(s, sums)
		passert.Equals(s, sums, expected...)
	}

	// Use fixed windows to divide the data into 3 chunks.
	validate(s.Scope("Fixed"), window.NewFixedWindows(windowSize), timestampedData, 15, 15, 15)
	// This should be identical to the "fixed" windows.
	validate(s.Scope("SlidingFixed"), window.NewSlidingWindows(windowSize, windowSize), timestampedData, 15, 15, 15)
	// This will have overlap, but each value should be a multiple of the magic number.
	validate(s.Scope("Sliding"), window.NewSlidingWindows(windowSize, 3*windowSize), timestampedData, 15, 30, 45, 30, 15)
	// With such a large gap, there should be a single session which will sum to 45.
	validate(s.Scope("Session"), window.NewSessions(windowSize), timestampedData, 45)
}

func sumPerKey(ws beam.Window, ts beam.EventTime, key beam.U, iter func(*int) bool) (beam.U, int) {
	var v, sum int
	for iter(&v) {
		sum += v
	}
	return key, sum
}

func gbkSumPerKey(s beam.Scope, in beam.PCollection) beam.PCollection {
	grouped := beam.GroupByKey(s, in)
	return beam.ParDo(s, sumPerKey, grouped)
}

func WindowSums_GBK(s beam.Scope) {
	WindowSums(s.Scope("GBK"), gbkSumPerKey)
}

func WindowSums_Lifted(s beam.Scope) {
	WindowSums(s.Scope("Lifted"), stats.SumPerKey)
}

// ValidateWindowedSideInputs checks that side inputs have accurate windowing information when used.
func ValidateWindowedSideInputs(s beam.Scope) {
	timestampedData := beam.ParDo(s, &createTimestampedData{Data: []int{1, 2, 3}}, beam.Impulse(s))

	timestampedData = beam.DropKey(s, timestampedData)

	windowSize := 1 * time.Second

	validateSums := func(s beam.Scope, wfn, sideFn *window.Fn, in, side beam.PCollection, expected ...any) {
		wData := beam.WindowInto(s, wfn, in)
		wSide := beam.WindowInto(s, sideFn, side)

		sums := beam.ParDo(s, sumSideInputs, wData, beam.SideInput{Input: wSide})

		sums = beam.WindowInto(s, window.NewGlobalWindows(), sums)

		passert.Equals(s, sums, expected...)
	}

	validateSums(s.Scope("Fixed-Global"), window.NewFixedWindows(windowSize), window.NewGlobalWindows(), timestampedData, timestampedData, 7, 8, 9)
	validateSums(s.Scope("Fixed-Same"), window.NewFixedWindows(windowSize), window.NewFixedWindows(windowSize), timestampedData, timestampedData, 2, 4, 6)
	validateSums(s.Scope("Fixed-Big"), window.NewFixedWindows(windowSize), window.NewFixedWindows(10*time.Second), timestampedData, timestampedData, 7, 8, 9)
	// Main: With window size 1, each window contains 1 element (1, 2, 3)
	// Side: Window size 2 with period 1, so each window covers 2 seconds of time
	//	 Have [1], [1,2], [2,3], [3]
	// Each main input should map to the earliest occuring sliding window it maps to:
	// (1, [1]) = 2
	// (2, [1, 2]) = 5
	// (3, [2, 3]) = 8
	validateSums(s.Scope("Fixed-Sliding"), window.NewFixedWindows(windowSize), window.NewSlidingWindows(windowSize, 2*windowSize), timestampedData, timestampedData, 2, 5, 8)
	// Main: Window size 2 with period 1, so each window has up to two elements
	//	 Have [1], [1,2], [2,3], [3]
	// Side: With window size 1, each window contains 1 element (1, 2, 3)
	// Each main input will map to the window its latest timestamp corresponds to:
	// ([1], 1) = 2
	// ([1, 2], 2) = 3, 4
	// ([2, 3], 3) = 5, 6
	// ([3], -) = 3
	validateSums(s.Scope("Sliding-Fixed"), window.NewSlidingWindows(windowSize, 2*windowSize), window.NewFixedWindows(windowSize), timestampedData, timestampedData, 2, 3, 4, 5, 6, 3)
}

func sumSideInputs(input int, iter func(*int) bool, emit func(int)) {
	var v, sum int
	sum += input
	for iter(&v) {
		sum += v
	}
	emit(sum)
}

func validateEquals(s beam.Scope, wfn *window.Fn, in beam.PCollection, opts []beam.WindowIntoOption, expected ...any) {
	windowed := beam.WindowInto(s, wfn, in, opts...)
	sums := stats.Sum(s, windowed)
	sums = beam.WindowInto(s, window.NewGlobalWindows(), sums)
	passert.Equals(s, sums, expected...)
}

// TriggerDefault tests the default trigger which fires the pane after the end of the window
func TriggerDefault(s beam.Scope) {
	con := teststream.NewConfig()
	con.AddElements(1000, 1.0, 2.0, 3.0)
	con.AdvanceWatermark(11000)
	con.AddElements(12000, 4.0, 5.0)
	con.AdvanceWatermark(13000)

	col := teststream.Create(s, con)
	windowSize := 10 * time.Second
	validateEquals(s.Scope("Fixed"), window.NewFixedWindows(windowSize), col,
		[]beam.WindowIntoOption{
			beam.Trigger(trigger.Default()),
		}, 6.0, 9.0)
}

// TriggerAlways tests the Always trigger, it is expected to receive every input value as the output.
func TriggerAlways(s beam.Scope) {
	con := teststream.NewConfig()
	con.AddElements(1000, 1.0, 2.0, 3.0)
	con.AdvanceWatermark(11000)
	col := teststream.Create(s, con)
	windowSize := 10 * time.Second

	validateEquals(s.Scope("Fixed"), window.NewFixedWindows(windowSize), col,
		[]beam.WindowIntoOption{
			beam.Trigger(trigger.Always()),
		}, 1.0, 2.0, 3.0)
}

// validateCount handles cases where we can only be sure of the count of elements
// and not their ordering.
func validateCount(s beam.Scope, wfn *window.Fn, in beam.PCollection, opts []beam.WindowIntoOption, expected int) {
	windowed := beam.WindowInto(s, wfn, in, opts...)
	sums := stats.Sum(s, windowed)
	sums = beam.WindowInto(s, window.NewGlobalWindows(), sums)
	passert.Count(s, sums, "total collections", expected)
}

// TriggerElementCount tests the ElementCount Trigger, it waits for atleast N elements to be ready
// to fire an output pane
func TriggerElementCount(s beam.Scope) {
	con := teststream.NewConfig()
	con.AddElements(1000, 1.0, 2.0, 3.0)
	con.AdvanceWatermark(2000)
	con.AddElements(6000, 4.0, 5.0)
	con.AdvanceWatermark(10000)
	con.AddElements(52000, 10.0)
	con.AdvanceWatermark(53000)

	col := teststream.Create(s, con)
	windowSize := 10 * time.Second

	// waits only for two elements to arrive and fires output after that and never fires that.
	// For the trigger to fire every 2 elements, combine it with Repeat Trigger
	validateCount(s.Scope("Fixed"), window.NewFixedWindows(windowSize), col,
		[]beam.WindowIntoOption{
			beam.Trigger(trigger.AfterCount(2)),
		}, 2)
}

// TriggerAfterProcessingTime tests the AfterProcessingTime Trigger, it fires output panes once 't' processing time has passed
// Not yet supported by the flink runner:
// java.lang.UnsupportedOperationException: Advancing Processing time is not supported by the Flink Runner.
func TriggerAfterProcessingTime(s beam.Scope) {
	con := teststream.NewConfig()
	con.AdvanceProcessingTime(100)
	con.AddElements(1000, 1.0, 2.0, 3.0)
	con.AdvanceProcessingTime(2000)
	con.AddElements(22000, 4.0)

	col := teststream.Create(s, con)

	validateEquals(s.Scope("Global"), window.NewGlobalWindows(), col,
		[]beam.WindowIntoOption{
			beam.Trigger(trigger.AfterProcessingTime().PlusDelay(5 * time.Second)),
		}, 6.0)
}

// TriggerRepeat tests the repeat trigger. As of now is it is configure to take only one trigger as a subtrigger.
// In the below test, it is expected to receive three output panes with two elements each.
func TriggerRepeat(s beam.Scope) {
	// create a teststream pipeline and get the pcollection
	con := teststream.NewConfig()
	con.AddElements(1000, 1.0, 2.0, 3.0)
	con.AdvanceWatermark(2000)
	con.AddElements(6000, 4.0, 5.0, 6.0)
	con.AdvanceWatermark(10000)

	col := teststream.Create(s, con)

	validateCount(s.Scope("Global"), window.NewGlobalWindows(), col,
		[]beam.WindowIntoOption{
			beam.Trigger(trigger.Repeat(trigger.AfterCount(2))),
		}, 3)
}

// TriggerAfterEndOfWindow tests the AfterEndOfWindow Trigger. With AfterCount(2) as the early firing trigger and AfterCount(1) as late firing trigger.
// It fires two times, one with early firing when there are two elements while the third elements waits in. This third element is fired in the late firing.
func TriggerAfterEndOfWindow(s beam.Scope) {
	con := teststream.NewConfig()
	con.AddElements(1000, 1.0, 2.0, 3.0)
	con.AdvanceWatermark(11000)

	col := teststream.Create(s, con)
	windowSize := 10 * time.Second
	trigger := trigger.AfterEndOfWindow().
		EarlyFiring(trigger.AfterCount(2)).
		LateFiring(trigger.AfterCount(1))

	validateCount(s.Scope("Fixed"), window.NewFixedWindows(windowSize), col,
		[]beam.WindowIntoOption{
			beam.Trigger(trigger),
		}, 2)
}

// TriggerAfterAll tests AfterAll trigger. The output pane is fired when all triggers in the subtriggers
// are ready. In this test, since trigger.AfterCount(int32(5)) won't be ready unless we see 5 elements,
// trigger.Always() won't fire until we meet that condition. So we fire only once when we see the 5th element.
func TriggerAfterAll(s beam.Scope) {
	con := teststream.NewConfig()
	con.AddElements(1000, 1.0, 2.0, 3.0, 5.0, 8.0)
	con.AdvanceWatermark(11000)

	col := teststream.Create(s, con)
	trigger := trigger.Repeat(
		trigger.AfterAll(
			[]trigger.Trigger{
				trigger.Always(),
				trigger.AfterCount(int32(5)),
			},
		),
	)

	validateCount(s.Scope("Global"), window.NewFixedWindows(10*time.Second), col,
		[]beam.WindowIntoOption{
			beam.Trigger(trigger),
		}, 1)
}

// TriggerAfterEach tests AfterEach trigger. The output pane is fired after each trigger
// is ready in the order set in subtriggers. In this test, since trigger.AfterCount(int32(3)) is first,
// first pane is fired after 3 elements, then a pane is fired each for trigger.Always() for
// element 5.0 and 8.0
func TriggerAfterEach(s beam.Scope) {
	con := teststream.NewConfig()
	con.AddElements(1000, 1.0, 2.0, 3.0, 5.0, 8.0)
	con.AdvanceWatermark(11000)

	col := teststream.Create(s, con)
	trigger := trigger.Repeat(
		trigger.AfterEach(
			[]trigger.Trigger{
				trigger.AfterCount(int32(3)),
				trigger.Always(),
			},
		),
	)

	validateCount(s.Scope("Global"), window.NewGlobalWindows(), col,
		[]beam.WindowIntoOption{
			beam.Trigger(trigger),
		}, 3)
}

// TriggerAfterAny tests AfterAny trigger. In this test, trigger.Always() gets ready everytime.
// So we would expect panes to be fired at every element irrespective of checking for other triggers.
func TriggerAfterAny(s beam.Scope) {
	con := teststream.NewConfig()
	con.AddElements(1000, 1.0, 2.0, 3.0)
	con.AdvanceWatermark(11000)
	con.AddElements(12000, 5.0, 8.0)

	col := teststream.Create(s, con)
	trigger := trigger.Repeat(
		trigger.AfterAny(
			[]trigger.Trigger{
				trigger.AfterCount(int32(3)),
				trigger.Always(),
			},
		),
	)
	windowSize := 10 * time.Second
	validateCount(s.Scope("Global"), window.NewFixedWindows(windowSize), col,
		[]beam.WindowIntoOption{
			beam.Trigger(trigger),
		}, 5)
}

// TriggerAfterSynchronizedProcessingTime tests AfterSynchronizedProcessingTime trigger. It fires at the window
// expiration since the times doesn't synchronize in this test case.
func TriggerAfterSynchronizedProcessingTime(s beam.Scope) {
	con := teststream.NewConfig()
	con.AddElements(1000, 1.0, 2.0, 3.0)
	con.AdvanceWatermark(11000)
	con.AddElements(12000, 5.0, 8.0)

	col := teststream.Create(s, con)
	trigger := trigger.Repeat(trigger.AfterSynchronizedProcessingTime())
	windowSize := 10 * time.Second
	validateCount(s.Scope("Global"), window.NewFixedWindows(windowSize), col,
		[]beam.WindowIntoOption{
			beam.Trigger(trigger),
		}, 2)
}

// TriggerNever tests Never Trigger. It fires at the window expiration.
func TriggerNever(s beam.Scope) {
	con := teststream.NewConfig()
	con.AddElements(1000, 1.0, 2.0, 3.0)
	con.AdvanceWatermark(11000)
	con.AddElements(12000, 5.0, 8.0)

	col := teststream.Create(s, con)
	trigger := trigger.Never()
	windowSize := 10 * time.Second
	validateCount(s.Scope("Global"), window.NewFixedWindows(windowSize), col,
		[]beam.WindowIntoOption{
			beam.Trigger(trigger),
		}, 2)
}

// TriggerOrFinally tests OrFinally trigger. The main trigger in this test case trigger.Always()
// is always ready. But the output is produced only when finally trigger is ready. So it is ready at second
// element in first window and produces two output panes. Similarly, for the second window.
func TriggerOrFinally(s beam.Scope) {
	con := teststream.NewConfig()
	con.AddElements(1000, 1.0, 2.0, 3.0)
	con.AdvanceWatermark(11000)
	con.AddElements(12000, 5.0, 8.0)

	col := teststream.Create(s, con)
	trigger := trigger.OrFinally(trigger.Always(), trigger.AfterCount(int32(2)))
	windowSize := 10 * time.Second
	validateCount(s.Scope("Global"), window.NewFixedWindows(windowSize), col,
		[]beam.WindowIntoOption{
			beam.Trigger(trigger),
		}, 4)
}
