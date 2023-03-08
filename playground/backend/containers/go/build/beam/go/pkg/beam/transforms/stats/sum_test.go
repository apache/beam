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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

// TestSumInt verifies that Sum adds ints correctly.
func TestSumInt(t *testing.T) {
	tests := []struct {
		in  []int
		exp []int
	}{
		{
			[]int{1, -2, 3},
			[]int{2},
		},
		{
			[]int{1, 11, 7, 5, 10},
			[]int{34},
		},
		{
			[]int{0},
			[]int{0},
		},
	}

	for _, test := range tests {
		p, s, in, exp := ptest.CreateList2(test.in, test.exp)
		passert.Equals(s, Sum(s, in), exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Sum(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}

// TestSumFloat verifies that Sum adds float32 correctly.
func TestSumFloat(t *testing.T) {
	tests := []struct {
		in  []float32
		exp []float32
	}{
		{
			[]float32{1, -2, 3.5},
			[]float32{2.5},
		},
		{
			[]float32{0, -99.99, 1, 1},
			[]float32{-97.99},
		},
		{
			[]float32{5.67890},
			[]float32{5.6789},
		},
	}

	for _, test := range tests {
		p, s, in, exp := ptest.CreateList2(test.in, test.exp)
		passert.Equals(s, Sum(s, in), exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Sum(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}

// TestSumKeyed verifies that Sum works correctly for KV values.
func TestSumKeyed(t *testing.T) {
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
			[]student{{"alpha", -3}, {"beta", 4}, {"charlie", 5.5}},
		},
	}

	for _, test := range tests {
		p, s, in, exp := ptest.CreateList2(test.in, test.exp)
		kv := beam.ParDo(s, studentToKV, in)
		sum := SumPerKey(s, kv)
		sumStudent := beam.ParDo(s, kvToStudent, sum)
		passert.Equals(s, sumStudent, exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Sum(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}
