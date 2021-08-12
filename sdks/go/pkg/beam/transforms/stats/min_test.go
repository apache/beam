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

// TestMinInt verifies that Min works correctly for ints.
func TestMinInt(t *testing.T) {
	tests := []struct {
		in  []int
		exp []int
	}{
		{
			[]int{1, -4, 3},
			[]int{-4},
		},
		{
			[]int{7, 11, 7, 5, 10},
			[]int{5},
		},
		{
			[]int{0, -2, -10},
			[]int{-10},
		},
		{
			[]int{-9},
			[]int{-9},
		},
	}

	for _, test := range tests {
		p, s, in, exp := ptest.CreateList2(test.in, test.exp)
		passert.Equals(s, Min(s, in), exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Min(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}

// TestMinFloat verifies that Min works correctly for float64s.
func TestMinFloat(t *testing.T) {
	tests := []struct {
		in  []float64
		exp []float64
	}{
		{
			[]float64{1, -2, 3.5},
			[]float64{-2},
		},
		{
			[]float64{0, -99.99, 1, 1},
			[]float64{-99.99},
		},
		{
			[]float64{5.67890},
			[]float64{5.6789},
		},
	}

	for _, test := range tests {
		p, s, in, exp := ptest.CreateList2(test.in, test.exp)
		passert.Equals(s, Min(s, in), exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Min(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}

// TestMinKeyed verifies that Min works correctly for KV values.
func TestMinKeyed(t *testing.T) {
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
			[]student{{"alpha", -4}, {"beta", 4}, {"charlie", 0}},
		},
	}

	for _, test := range tests {
		p, s, in, exp := ptest.CreateList2(test.in, test.exp)
		kv := beam.ParDo(s, studentToKV, in)
		min := MinPerKey(s, kv)
		minStudent := beam.ParDo(s, kvToStudent, min)
		passert.Equals(s, minStudent, exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Min(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}
