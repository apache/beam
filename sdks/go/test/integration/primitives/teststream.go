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

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/teststream"
)

// TestStreamSequence tests the TestStream primitive by inserting string elements
// then advancing the watermark past the point where they were inserted.
func TestStreamStrings(s beam.Scope) {
	con := teststream.NewConfig()
	con.AddElements(100, "a", "b", "c")
	con.AdvanceWatermarkToInfinity()

	col := teststream.Create(s, con)

	passert.Count(s, col, "teststream strings", 3)
	passert.Equals(s, col, "a", "b", "c")
}

// TestStreamByteSliceSequence tests the TestStream primitive by inserting byte slice elements
// then advancing the watermark to infinity and comparing the output..
func TestStreamByteSliceSequence(s beam.Scope) {
	con := teststream.NewConfig()

	a := []byte{91, 92, 93}
	b := []byte{94, 95, 96}
	c := []byte{97, 98, 99}
	con.AddElements(1, a, b, c)
	con.AdvanceWatermarkToInfinity()
	col := teststream.Create(s, con)
	passert.Count(s, col, "teststream byte", 3)
	passert.Equals(s, col, a, b, c)
}

// TestStreamInt64Sequence tests the TestStream primitive by inserting int64 elements
// then advancing the watermark past the point where they were inserted.
func TestStreamInt64Sequence(s beam.Scope) {
	con := teststream.NewConfig()
	ele := []int64{91, 92, 93}
	con.AddElementList(100, ele)
	con.AdvanceWatermarkToInfinity()

	col := teststream.Create(s, con)

	passert.Count(s, col, "teststream int64", 3)
	passert.EqualsList(s, col, ele)
}

// TestStreamTwoInt64Sequences tests the TestStream primitive by inserting two sets of
// int64 elements that arrive on-time into the TestStream
func TestStreamTwoInt64Sequences(s beam.Scope) {
	con := teststream.NewConfig()
	eo := []int64{91, 92, 93}
	et := []int64{96, 97, 98}
	con.AddElementList(100, eo)
	con.AdvanceWatermark(110)
	con.AddElementList(120, et)
	con.AdvanceWatermark(130)

	col := teststream.Create(s, con)

	passert.Count(s, col, "teststream int64", 6)
	passert.EqualsList(s, col, append(eo, et...))
}

// TestStreamFloat64Sequence tests the TestStream primitive by inserting float64 elements
// then advancing the watermark past the point where they were inserted.
func TestStreamFloat64Sequence(s beam.Scope) {
	con := teststream.NewConfig()
	ele := []float64{91.1, 92.2, 93.3}
	con.AddElementList(100, ele)
	con.AdvanceWatermarkToInfinity()

	col := teststream.Create(s, con)

	passert.Count(s, col, "teststream float64", 3)
	passert.EqualsList(s, col, ele)
}

// TestStreamTwoFloat64Sequences tests the TestStream primitive by inserting two sets of
// float64 elements that arrive on-time into the TestStream
func TestStreamTwoFloat64Sequences(s beam.Scope) {
	con := teststream.NewConfig()
	eo := []float64{91.1, 92.2, 93.3}
	et := []float64{96.4, 97.5, 98.6}
	con.AddElementList(100, eo)
	con.AdvanceWatermark(110)
	con.AddElementList(120, et)
	con.AdvanceWatermark(130)

	col := teststream.Create(s, con)

	passert.Count(s, col, "teststream float64", 6)
	passert.EqualsList(s, col, append(eo, et...))
}

// TestStreamBoolSequence tests the TestStream primitive by inserting boolean elements
// then advancing the watermark past the point where they were inserted.
func TestStreamBoolSequence(s beam.Scope) {
	con := teststream.NewConfig()
	ele := []bool{true, false, true}
	con.AddElementList(100, ele)
	con.AdvanceWatermarkToInfinity()

	col := teststream.Create(s, con)

	passert.Count(s, col, "teststream bool", 3)
	passert.EqualsList(s, col, ele)
}

// TestStreamTwoBoolSequences tests the TestStream primitive by inserting two sets of
// boolean elements that arrive on-time into the TestStream
func TestStreamTwoBoolSequences(s beam.Scope) {
	con := teststream.NewConfig()
	eo := []bool{true, false, true}
	et := []bool{false, true, false}
	con.AddElementList(100, eo)
	con.AdvanceWatermark(110)
	con.AddElementList(120, et)
	con.AdvanceWatermark(130)

	col := teststream.Create(s, con)

	passert.Count(s, col, "teststream bool", 6)
	passert.EqualsList(s, col, append(eo, et...))
}

// TestStreamTwoUserTypeSequences tests the TestStream primitive by inserting two sets of
// boolean elements that arrive on-time into the TestStream
func TestStreamTwoUserTypeSequences(s beam.Scope) {
	con := teststream.NewConfig()
	eo := []stringPair{{"a", "b"}, {"b", "c"}, {"c", "a"}}
	et := []stringPair{{"b", "a"}, {"c", "b"}, {"a", "c"}}
	con.AddElementList(100, eo)
	con.AdvanceWatermark(110)
	con.AddElementList(120, et)
	con.AdvanceWatermark(130)

	col := teststream.Create(s, con)

	passert.Count(s, col, "teststream usertype", 6)
	passert.EqualsList(s, col, append(eo, et...))
}

// TestStreamInt16Sequence validates that a non-beam standard coder
// works with test stream.
func TestStreamInt16Sequence(s beam.Scope) {
	con := teststream.NewConfig()
	ele := []int16{91, 92, 93}
	con.AddElementList(100, ele)
	con.AdvanceWatermarkToInfinity()

	col := teststream.Create(s, con)

	passert.Count(s, col, "teststream int15", 3)
	passert.EqualsList(s, col, ele)
}

// panicIfNot42 panics if the value is not 42.
func panicIfNot42(v int) {
	if v != 42 {
		panic(fmt.Sprintf("got %v, want 42", v))
	}
}

// dropKeyEmitValues drops the key and emits the value.
func dropKeyEmitValues(_ int, vs func(*int) bool, emit func(int)) {
	var v int
	for vs(&v) {
		emit(v)
	}
}

func init() {
	register.Function1x0(panicIfNot42)
	register.Function3x0(dropKeyEmitValues)
}

// TestStreamSimple is a trivial pipeline where teststream sends
// a single element to a DoFn that checks that it's received the value.
// Intended for runner validation.
func TestStreamSimple(s beam.Scope) {
	con := teststream.NewConfig()
	ele := []int{42}
	con.AddElementList(100, ele)
	con.AdvanceWatermarkToInfinity()

	col := teststream.Create(s, con)
	beam.ParDo0(s, panicIfNot42, col)
}

// TestStreamSimple_InfinityDefault is the same trivial pipeline that
// validates that the watermark is automatically advanced to infinity
// even when the user doesn't set it.
// Intended for runner validation.
func TestStreamSimple_InfinityDefault(s beam.Scope) {
	con := teststream.NewConfig()
	ele := []int{42}
	con.AddElementList(100, ele)

	col := teststream.Create(s, con)
	beam.ParDo0(s, panicIfNot42, col)
}

// TestStreamToGBK is a trivial pipeline where teststream sends
// a single element to a GBK.
func TestStreamToGBK(s beam.Scope) {
	con := teststream.NewConfig()
	ele := []int{42}
	con.AddElementList(100, ele)
	con.AdvanceWatermarkToInfinity()

	col := teststream.Create(s, con)
	keyed := beam.AddFixedKey(s, col)
	gbk := beam.GroupByKey(s, keyed)
	dropped := beam.ParDo(s, dropKeyEmitValues, gbk)
	beam.ParDo0(s, panicIfNot42, dropped)
}

// TestStreamTimersEventTime validates event time timers in a test stream "driven" pipeline.
func TestStreamTimersEventTime(s beam.Scope) {
	timersEventTimePipelineBuilder(func(s beam.Scope) beam.PCollection {
		c := teststream.NewConfig()
		c.AddElements(123456, []byte{42})
		c.AdvanceWatermarkToInfinity()
		return teststream.Create(s, c)
	})(s)
}
