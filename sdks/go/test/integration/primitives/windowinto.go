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
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/teststream"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
)

func init() {
	beam.RegisterFunction(sumPerKey)
	beam.RegisterType(reflect.TypeOf((*createTimestampedData)(nil)).Elem())
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

	validate := func(s beam.Scope, wfn *window.Fn, in beam.PCollection, expected ...interface{}) {
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

func validate(s beam.Scope, wfn *window.Fn, in beam.PCollection, tr window.Trigger, m window.AccumulationMode, expected ...interface{}) {
	windowed := beam.WindowInto(s, wfn, in, beam.WindowTrigger{Name: tr}, beam.AccumulationMode{Mode: m})
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
	validate(s.Scope("Fixed"), window.NewFixedWindows(windowSize), col, window.Trigger{Kind: window.DefaultTrigger}, window.Accumulating, 6.0, 9.0)
}

// TriggerAlways tests the Always trigger, it is expected to receive every input value as the output.
func TriggerAlways(s beam.Scope) {
	con := teststream.NewConfig()
	con.AddElements(1000, 1.0, 2.0, 3.0)
	con.AdvanceWatermark(11000)
	col := teststream.Create(s, con)
	windowSize := 10 * time.Second

	validate(s.Scope("Fixed"), window.NewFixedWindows(windowSize), col, window.Trigger{Kind: window.AlwaysTrigger}, window.Discarding, 1.0, 2.0, 3.0)
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

	// waits only for two elements to arrive and fires output after that and never fires that.
	// For the trigger to fire every 2 elements, combine it with Repeat Trigger
	tr := window.Trigger{Kind: window.ElementCountTrigger, ElementCount: 2}
	windowed := beam.WindowInto(s, window.NewGlobalWindows(), col, beam.WindowTrigger{Name: tr}, beam.AccumulationMode{Mode: window.Discarding})
	sums := stats.Sum(s, windowed)
	sums = beam.WindowInto(s, window.NewGlobalWindows(), sums)
	passert.Count(s, sums, "total collections", 1)
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

	validate(s.Scope("Fixed"), window.NewGlobalWindows(), col, window.Trigger{Kind: window.AfterProcessingTimeTrigger, Delay: 5000}, window.Discarding, 6.0)
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

	subTr := window.Trigger{Kind: window.ElementCountTrigger, ElementCount: 2}
	tr := window.Trigger{Kind: window.RepeatTrigger, SubTriggers: []window.Trigger{subTr}}
	windowed := beam.WindowInto(s, window.NewGlobalWindows(), col, beam.WindowTrigger{Name: tr}, beam.AccumulationMode{Mode: window.Discarding})
	sums := stats.Sum(s, windowed)
	sums = beam.WindowInto(s, window.NewGlobalWindows(), sums)
	passert.Count(s, sums, "total collections", 3)
}
