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

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
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

// WindowsSums produces a pipeline that generates the numbers of a 3x3 magic square, and
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
	// TODO(BEAM-4152): This will do a smoke test of session windows, once implemented through.
	// With such a large gap, there should be a single session which will sum to 45.
	// validate(s.Scope("Session"), window.NewSessions(windowSize), timestampedData, 45)
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
