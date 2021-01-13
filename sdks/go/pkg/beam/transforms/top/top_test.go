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

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

// TestCombineFn3String verifies that the accumulator correctly
// maintains the top 3 longest strings.
func TestCombineFn3String(t *testing.T) {
	less := func(a, b string) bool {
		return len(a) < len(b)
	}
	fn := newCombineFn(less, 3, reflectx.String, false)

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
	less := func(a, b string) bool {
		return len(a) < len(b)
	}
	fn := newCombineFn(less, 3, reflectx.String, true)

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
	less := func(a, b string) bool {
		return len(a) < len(b)
	}
	fn := newCombineFn(less, 3, reflectx.String, false)
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
	for i, b := range as {
		buf, err := b.MarshalJSON()
		if err != nil {
			t.Fatalf("failure marshalling accum[%d]: %v, %+v", i, err, b)
		}
		var c accum
		if err := c.UnmarshalJSON(buf); err != nil {
			t.Fatalf("failure unmarshalling accum[%d]: %v, %+v", i, err, b)
		}
		a = fn.MergeAccumulators(a, c)
	}
	return a
}

func outputUnmarshal(t *testing.T, fn *combineFn, a accum) []string {
	buf, err := a.MarshalJSON()
	if err != nil {
		t.Fatalf("failure marshalling accum: %v, %+v", err, a)
	}
	var b accum
	if err := b.UnmarshalJSON(buf); err != nil {
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
