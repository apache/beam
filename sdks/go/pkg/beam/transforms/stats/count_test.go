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
	"fmt"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
)

type count struct {
	Elm   int
	Count int
}

func kvToCount(e int, c int) count {
	return count{e, c}
}

// TestCountInt verifies that Count works correctly for ints.
func TestCountInt(t *testing.T) {
	tests := []struct {
		in  []int
		exp []count
	}{
		{
			[]int{0},
			[]count{{0, 1}},
		},
		{
			[]int{0, 0},
			[]count{{0, 2}},
		},
		{
			[]int{1, -4, 1, -1},
			[]count{{1, 2}, {-1, 1}, {-4, 1}},
		},
		{
			[]int{1, -1, 1, -1, 1},
			[]count{{1, 3}, {-1, 2}},
		},
	}

	for _, test := range tests {
		p, s, in, exp := ptest.CreateList2(test.in, test.exp)
		count := Count(s, in)
		formatted := beam.ParDo(s, kvToCount, count)
		passert.Equals(s, formatted, exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Count(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}

// TestCountElms verifies that CountElms works correctly with PCollections of
// various types, including keyed and unkeyed elements.
func TestCountElms(t *testing.T) {

	tests := []struct {
		name  string
		in    func(s beam.Scope) beam.PCollection
		count int
	}{
		{
			name: "empty",
			in: func(s beam.Scope) beam.PCollection {
				return beam.CreateList(s, []int{})
			},
			count: 0,
		},
		{
			name: "single",
			in: func(s beam.Scope) beam.PCollection {
				return beam.Create(s, 1)
			},
			count: 1,
		},
		{
			name: "multiple",
			in: func(s beam.Scope) beam.PCollection {
				return beam.Create(s, "one", "two", "three")
			},
			count: 3,
		},
		{
			name: "keyed",
			in: func(s beam.Scope) beam.PCollection {
				vals := beam.Create(s, 1.0, 2.0, 3.0, 4.0, 5.0)
				return beam.AddFixedKey(s, vals)
			},
			count: 5,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("_%v", test.name), func(t *testing.T) {
			p := beam.NewPipeline()
			s := p.Root()
			in := test.in(s)
			exp := beam.Create(s, test.count)
			count := CountElms(s, in)
			passert.Equals(s, count, exp)

			if err := ptest.Run(p); err != nil {
				t.Errorf("CountElms != %v: %v", test.count, err)
			}
		})
	}
}
