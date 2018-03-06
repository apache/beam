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

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
)

// TestMeanInt verifies that Mean works correctly for ints.
func TestMeanInt(t *testing.T) {
	tests := []struct {
		in  []int
		exp []float64
	}{
		{
			[]int{1, -4},
			[]float64{-1.5},
		},
		{
			[]int{7, 11, 7, 5, 10},
			[]float64{8},
		},
		{
			[]int{0, -2, -10},
			[]float64{-4},
		},
		{
			[]int{-9},
			[]float64{-9},
		},
	}

	for _, test := range tests {
		p, s, in, exp := ptest.CreateList2(test.in, test.exp)
		passert.Equals(s, Mean(s, in), exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Mean(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}

// TestMeanFloat verifies that Mean works correctly for float64s.
func TestMeanFloat(t *testing.T) {
	tests := []struct {
		in  []float64
		exp []float64
	}{
		{
			[]float64{1, -2, 3.5, 1},
			[]float64{0.875},
		},
		{
			[]float64{0, -99.99, 1, 1},
			[]float64{-24.4975},
		},
		{
			[]float64{5.67890},
			[]float64{5.6789},
		},
	}

	for _, test := range tests {
		p, s, in, exp := ptest.CreateList2(test.in, test.exp)
		passert.Equals(s, Mean(s, in), exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Mean(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}

// TestMeanKeyed verifies that Mean works correctly for KV values.
func TestMeanKeyed(t *testing.T) {
	tests := []struct {
		in  []student
		exp []student
	}{
		{
			[]student{{"alpha", 1}, {"beta", 4}, {"charlie", 3.5}},
			[]student{{"alpha", 1}, {"beta", 4}, {"charlie", 3.5}},
		},
		{
			[]student{{"alpha", 1}},
			[]student{{"alpha", 1}},
		},
		{
			[]student{{"alpha", 1}, {"alpha", -4}, {"beta", 4}, {"charlie", 0}, {"charlie", 5.5}},
			[]student{{"alpha", -1.5}, {"beta", 4}, {"charlie", 2.75}},
		},
	}

	for _, test := range tests {
		p, s, in, exp := ptest.CreateList2(test.in, test.exp)
		kv := beam.ParDo(s, studentToKV, in)
		mean := MeanPerKey(s, kv)
		meanStudent := beam.ParDo(s, kvToStudent, mean)
		passert.Equals(s, meanStudent, exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Mean(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}
