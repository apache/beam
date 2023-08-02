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

package top

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

func init() {
	register.Function2x2(addKeyFn)
	register.Function2x1(lessInt)
	register.Function2x1(shorterString)
}

func lessInt(a, b int) bool {
	return a < b
}

func shorterString(a, b string) bool {
	return len(a) < len(b)
}

// TestCombineFn3String verifies that the accumulator correctly
// maintains the top 3 longest strings.
func TestCombineFn3String(t *testing.T) {
	fn := newCombineFn(shorterString, 3, reflectx.String, false)

	tests := []struct {
		Elms     []string
		Expected []string
	}{
		{[]string{}, nil},
		{[]string{"foo"}, []string{"foo"}},
		{[]string{"1", "2", "3", "4", "5"}, []string{"1", "2", "3"}},
		{[]string{"a1", "b22", "c22", "d333", "e22"}, []string{"d333", "b22", "c22"}},
	}

	for _, test := range tests {
		a := load(fn, test.Elms...)

		actual := output(fn, a)
		if !reflect.DeepEqual(actual, test.Expected) {
			t.Errorf("CombineFn(3; %v) = %v, want %v", test.Elms, actual, test.Expected)
		}
	}
}

// TestCombineFn3RevString verifies that the accumulator correctly
// maintains the top 3 shortest strings.
func TestCombineFn3RevString(t *testing.T) {
	fn := newCombineFn(shorterString, 3, reflectx.String, true)

	tests := []struct {
		Elms     []string
		Expected []string
	}{
		{[]string{}, nil},
		{[]string{"foo"}, []string{"foo"}},
		{[]string{"1", "2", "3", "4", "5"}, []string{"1", "2", "3"}},
		{[]string{"a1", "b22", "c22", "d333", "e22"}, []string{"a1", "b22", "c22"}},
	}

	for _, test := range tests {
		a := load(fn, test.Elms...)

		actual := output(fn, a)
		if !reflect.DeepEqual(actual, test.Expected) {
			t.Errorf("CombineFn(3; %v) = %v, want %v", test.Elms, actual, test.Expected)
		}
	}
}

// TestCombineFnMerge verifies that accumulators merge correctly and
// extractOutput still works on the marshalled accumulators it receives after
// merging.
func TestCombineFnMerge(t *testing.T) {
	fn := newCombineFn(shorterString, 3, reflectx.String, false)
	tests := []struct {
		Elms     [][]string
		Expected []string
	}{
		{[][]string{nil}, nil},
		{[][]string{{"foo"}}, []string{"foo"}},
		{[][]string{{"1", "2"}, {"3"}, {"4", "5"}, {"6", "7"}}, []string{"1", "2", "3"}},
		{[][]string{{"a1"}, {"b22", "c22"}, {"d333"}, {"e22"}}, []string{"d333", "b22", "c22"}},
		{[][]string{{"a55555"}, {"b22", "c4444"}, {"d333"}, {"e22"}}, []string{"a55555", "c4444", "d333"}},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%02d", i), func(t *testing.T) {
			var list []accum
			for _, a := range test.Elms {
				list = append(list, load(fn, a...))
			}
			a := merge(t, fn, list...)
			actual := outputUnmarshal(t, fn, a)
			if !reflect.DeepEqual(actual, test.Expected) {
				t.Errorf("CombineFn(3; %v) = %v, want %v", test.Elms, actual, test.Expected)
			}
		})
	}
}

func load(fn *combineFn, elms ...string) accum {
	a := fn.CreateAccumulator()
	for _, elm := range elms {
		a = fn.AddInput(a, elm)
	}
	return a
}

func merge(t *testing.T, fn *combineFn, as ...accum) accum {
	t.Helper()
	a := fn.CreateAccumulator()
	enc := accumEnc()
	dec := accumDec()

	for i, b := range as {
		buf, err := enc(b)
		if err != nil {
			t.Fatalf("failure marshalling accum[%d]: %v, %+v", i, err, b)
		}
		c, err := dec(buf)
		if err != nil {
			t.Fatalf("failure unmarshalling accum[%d]: %v, %+v", i, err, b)
		}
		a = fn.MergeAccumulators(a, c)
	}
	return a
}

func outputUnmarshal(t *testing.T, fn *combineFn, a accum) []string {
	enc := accumEnc()
	dec := accumDec()

	buf, err := enc(a)
	if err != nil {
		t.Fatalf("failure marshalling accum: %v, %+v", err, a)
	}
	b, err := dec(buf)
	if err != nil {
		t.Fatalf("failure unmarshalling accum: %v, %+v", err, b)
	}
	return output(fn, b)
}

func output(fn *combineFn, a accum) []string {
	var ret []string
	for _, actual := range fn.ExtractOutput(a) {
		ret = append(ret, actual.(string))
	}
	return ret
}

// TestLargest checks that the Largest transform outputs the correct elements
// for a given PCollection of ints and a comparator function.
func TestLargest(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, 1, 11, 7, 5, 10)
	topTwo := Largest(s, col, 2, lessInt)
	passert.Equals(s, topTwo, []int{11, 10})
	if err := ptest.Run(p); err != nil {
		t.Errorf("pipeline failed but should have succeeded, got %v", err)
	}
}

// TestSmallest checks that the Smallest transform outputs the correct elements
// for a given PCollection of ints and a comparator function.
func TestSmallest(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, 1, 11, 7, 5, 10)
	botTwo := Smallest(s, col, 2, lessInt)
	passert.Equals(s, botTwo, []int{1, 5})
	if err := ptest.Run(p); err != nil {
		t.Errorf("pipeline failed but should have succeeded, got %v", err)
	}
}

func addKey(s beam.Scope, col beam.PCollection, newKey int) beam.PCollection {
	keyCol := beam.Create(s, newKey)
	return beam.ParDo(s, addKeyFn, col, beam.SideInput{Input: keyCol})
}

func addKeyFn(elm beam.T, newKey int) (int, beam.T) {
	return newKey, elm
}

// TestLargestPerKey ensures that the LargestPerKey transform outputs the proper
// collection for a PCollection of type <int, int>.
func TestLargestPerKey(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	colZero := beam.Create(s, 1, 11, 7, 5, 10)
	keyedZero := addKey(s, colZero, 0)

	colOne := beam.Create(s, 2, 12, 8, 6, 11)
	keyedOne := addKey(s, colOne, 1)

	col := beam.Flatten(s, keyedZero, keyedOne)
	top := LargestPerKey(s, col, 2, lessInt)
	out := beam.DropKey(s, top)
	passert.Equals(s, out, []int{11, 10}, []int{12, 11})
	if err := ptest.Run(p); err != nil {
		t.Errorf("pipeline failed but should have succeeded, got %v", err)
	}
}

// TestSmallestPerKey ensures that the SmallestPerKey transform outputs the proper
// collection for a PCollection of type <int, int>.
func TestSmallestPerKey(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	colZero := beam.Create(s, 1, 11, 7, 5, 10)
	keyedZero := addKey(s, colZero, 0)

	colOne := beam.Create(s, 2, 12, 8, 6, 11)
	keyedOne := addKey(s, colOne, 1)

	col := beam.Flatten(s, keyedZero, keyedOne)
	bot := SmallestPerKey(s, col, 2, lessInt)
	out := beam.DropKey(s, bot)
	passert.Equals(s, out, []int{1, 5}, []int{2, 6})
	if err := ptest.Run(p); err != nil {
		t.Errorf("pipeline failed but should have succeeded, got %v", err)
	}
}
