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

package stats

import (
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/google/go-cmp/cmp"
)

func init() {
	register.Function1x2(weightedElementToKv)
	register.Function2x1(less)
}

func less(a, b int) bool {
	return a < b
}

func TestLargeQuantiles(t *testing.T) {
	const numElements int = 30000
	inputSlice := make([]int, 0, numElements)
	for i := 0; i < numElements; i++ {
		inputSlice = append(inputSlice, i)
	}
	p, s, input, expected := ptest.CreateList2(inputSlice, [][]int{{10006, 19973}})
	quantiles := ApproximateQuantiles(s, input, less, Opts{
		K:            200,
		NumQuantiles: 3,
	})
	passert.Equals(s, quantiles, expected)
	if err := ptest.Run(p); err != nil {
		t.Errorf("ApproximateQuantiles failed: %v", err)
	}
}

func TestLargeQuantilesReversed(t *testing.T) {
	const numElements int = 30000
	inputSlice := make([]int, 0, numElements)
	for i := numElements - 1; i >= 0; i-- {
		inputSlice = append(inputSlice, i)
	}
	p, s, input, expected := ptest.CreateList2(inputSlice, [][]int{{9985, 19959}})
	quantiles := ApproximateQuantiles(s, input, less, Opts{
		K:            200,
		NumQuantiles: 3,
	})
	passert.Equals(s, quantiles, expected)
	if err := ptest.Run(p); err != nil {
		t.Errorf("ApproximateQuantiles failed: %v", err)
	}
}

func TestBasicQuantiles(t *testing.T) {
	// Test asking for 3 quantiles for k=3.
	tests := []struct {
		Input    []int
		Expected [][]int
	}{
		{[]int{}, [][]int{}},
		{[]int{1}, [][]int{{1}}},
		{[]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, [][]int{{6, 13}}},
	}

	for _, test := range tests {
		p, s, in, exp := ptest.CreateList2(test.Input, test.Expected)
		quantiles := ApproximateQuantiles(s, in, less, Opts{
			K:            3,
			NumQuantiles: 3,
		})
		passert.Equals(s, quantiles, exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("ApproximateQuantiles(%v) != %v: %v", test.Input, test.Expected, err)
		}
	}
}

func weightedElementToKv(e testWeightedElement) (int, int) {
	return e.Weight, e.Element
}

type testWeightedElement struct {
	Weight  int
	Element int
}

func TestWeightedQuantiles(t *testing.T) {
	// Test asking for 3 quantiles for k=3.
	input := []testWeightedElement{
		{Weight: 1, Element: 1},
		{Weight: 10, Element: 2},
		{Weight: 1, Element: 3},
		{Weight: 10, Element: 4}}
	expected := []int{2, 4}
	p, s, in, exp := ptest.CreateList2(input, [][]int{expected})
	elementWeightKvs := beam.ParDo(s, weightedElementToKv, in, beam.TypeDefinition{Var: beam.TType, T: reflectx.Int})
	opts := Opts{
		K:            3,
		NumQuantiles: 3,
	}
	quantiles := ApproximateWeightedQuantiles(s, elementWeightKvs, less, opts)
	passert.Equals(s, quantiles, exp)

	if err := ptest.Run(p); err != nil {
		t.Errorf("ApproximateQuantiles(%v) != %v: %v", input, expected, err)
	}
}

func TestWeightedQuantilesWithInternalSharding(t *testing.T) {
	// Test shard reduction.
	input := []testWeightedElement{
		{Weight: 1, Element: 1},
		{Weight: 10, Element: 2},
		{Weight: 1, Element: 3},
		{Weight: 10, Element: 4}}
	expected := []int{2, 4}
	p, s, in, exp := ptest.CreateList2(input, [][]int{expected})
	elementWeightKvs := beam.ParDo(s, weightedElementToKv, in, beam.TypeDefinition{Var: beam.TType, T: reflectx.Int})
	opts := Opts{
		K:                3,
		NumQuantiles:     3,
		InternalSharding: []int{4, 3, 2},
	}
	quantiles := ApproximateWeightedQuantiles(s, elementWeightKvs, less, opts)
	passert.Equals(s, quantiles, exp)

	if err := ptest.Run(p); err != nil {
		t.Errorf("ApproximateQuantiles(%v) != %v: %v", input, expected, err)
	}
}

func TestMerging(t *testing.T) {
	compactors1 := compactors{
		K:                   3,
		NumberOfCompactions: 1,
		Compactors: []compactor{{
			sorted:   [][]beam.T{{1}, {2}, {3}},
			unsorted: []beam.T{6, 5, 4},
			capacity: 4,
		}},
	}

	compactors2 := compactors{
		K:                   3,
		NumberOfCompactions: 1,
		Compactors: []compactor{
			{
				sorted:   [][]beam.T{{7}, {8}, {9}},
				unsorted: []beam.T{12, 11, 10},
				capacity: 4},
		},
	}

	compactors1.merge(&compactors2, reflectx.MakeFunc2x1(less))

	expectedCompactors := compactors{
		K:                   3,
		NumberOfCompactions: 3,
		Compactors: []compactor{
			{capacity: 4},
			{
				sorted:   [][]beam.T{{1, 3, 5, 7, 9, 11}},
				capacity: 4,
			},
		},
	}
	if d := cmp.Diff(expectedCompactors, compactors1, cmp.AllowUnexported(compactor{})); d != "" {
		t.Errorf("Failed. Expected %v, was %v, diff: %v", expectedCompactors, compactors1, d)
	}
}

func TestCompactorsEncoding(t *testing.T) {
	compactors := compactors{
		K:                   3,
		NumberOfCompactions: 1,
		Compactors: []compactor{
			{
				capacity: 4,
				sorted:   [][]beam.T{{1, 2}},
				unsorted: []beam.T{3, 4},
			},
			{
				capacity: 4,
				sorted:   [][]beam.T{{5, 6}},
				unsorted: []beam.T{7, 8},
			},
		},
	}
	compactors.Compactors[0].update(1)
	data, err := encodeCompactors(&compactors)
	if err != nil {
		t.Errorf("Failed to encode, %v", err)
	}
	decodedCompactors, err := decodeCompactors(data)
	if err != nil {
		t.Errorf("Failed to decode, %v", err)
	}
	// We want to use cmp.Diff which makes a distinction between empty and nil slices.
	// So we need to clean up empty slices to be nil.
	for i := range decodedCompactors.Compactors {
		if len(decodedCompactors.Compactors[i].sorted) == 0 {
			decodedCompactors.Compactors[i].sorted = nil
		}
		if len(decodedCompactors.Compactors[i].unsorted) == 0 {
			decodedCompactors.Compactors[i].unsorted = nil
		}
	}
	if d := cmp.Diff(&compactors, decodedCompactors, cmp.AllowUnexported(compactor{})); d != "" {
		t.Errorf("Invalid coder. Wanted %v, got %v, diff: %v", &compactors, decodedCompactors, d)
	}
}

func TestWeightedElementEncoding(t *testing.T) {
	w := weightedElement{
		weight:  10,
		element: 1,
	}
	data, err := encodeWeightedElement(w)
	if err != nil {
		t.Errorf("Failed to encode %v", err)
	}
	decoded, err := decodeWeightedElement(data)
	if err != nil {
		t.Errorf("Failed to decode %v", err)
	}
	if d := cmp.Diff(w, decoded, cmp.AllowUnexported(weightedElement{})); d != "" {
		t.Errorf("Invalid coder. Wanted %v got %v", w, decoded)
	}
}
