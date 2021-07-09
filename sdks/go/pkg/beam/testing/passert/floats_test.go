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

package passert

import (
	"strings"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
)

func TestAllWithinBounds_GoodFloats(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, 0.0, 0.5, 1.0)
	AllWithinBounds(s, col, 0.0, 1.0)
	err := ptest.Run(p)
	if err != nil {
		t.Errorf("Pipeline failed: %v", err)
	}
}

func TestAllWithinBounds_GoodInts(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, 0, 1, 2)
	AllWithinBounds(s, col, 0.0, 2.0)
	err := ptest.Run(p)
	if err != nil {
		t.Errorf("Pipeline failed: %v", err)
	}
}

func TestAllWithinBounds_FlippedBounds(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, 0.0, 0.5, 1.0)
	AllWithinBounds(s, col, 1.0, 0.0)
	err := ptest.Run(p)
	if err != nil {
		t.Errorf("Pipeline failed: %v", err)
	}
}

func TestAllWithinBounds_BadFloats(t *testing.T) {
	var badBoundsTests = []struct {
		name       string
		inputs     []float64
		lo         float64
		hi         float64
		errorParts []string
	}{
		{
			"out of int bounds low",
			[]float64{-1.0, 0.5, 1.0},
			0,
			1,
			[]string{"values below minimum value 0:", "[-1]"},
		},
		{
			"out of int bounds high",
			[]float64{0.0, 0.5, 2.0},
			0,
			1,
			[]string{"values above maximum value 1:", "[2]"},
		},
		{
			"out of int bounds both",
			[]float64{-0.5, 0.0, 0.5, 2.0},
			0,
			1,
			[]string{"values below minimum value 0:", "[-0.5]", "values above maximum value 1:", "[2]"},
		},
		{
			"out of int bounds in-order",
			[]float64{-0.5, -1.0, 0.0, 0.5},
			0,
			1,
			[]string{"values below minimum value 0:", "[-1 -0.5]"},
		},
		{
			"out of float bounds low",
			[]float64{0.2, 1.0, 1.5},
			0.5,
			1.5,
			[]string{"values below minimum value 0.5:", "[0.2]"},
		},
		{
			"out of float bounds high",
			[]float64{0.5, 1.0, 2.0},
			0.5,
			1.5,
			[]string{"values above maximum value 1.5:", "[2]"},
		},
		{
			"out of float bounds both",
			[]float64{0.0, 0.5, 1.0, 2.0},
			0.5,
			1.5,
			[]string{"values below minimum value 0.5:", "[0]", "values above maximum value 1.5:", "[2]"},
		},
		{
			"out of float bounds in-order",
			[]float64{0.5, 1.0, 2.0, 2.5},
			0.5,
			1.5,
			[]string{"values above maximum value 1.5:", "[2 2.5]"},
		},
	}
	for _, tc := range badBoundsTests {
		p, s := beam.NewPipelineWithRoot()
		col := beam.CreateList(s, tc.inputs)
		AllWithinBounds(s, col, tc.lo, tc.hi)
		err := ptest.Run(p)
		if err == nil {
			t.Fatalf("Pipeline succeeded but should have failed.")
		}
		str := err.Error()
		missing := []string{}
		for _, part := range tc.errorParts {
			if !strings.Contains(str, part) {
				missing = append(missing, part)
			}
		}
		if len(missing) != 0 {
			t.Errorf("%v: pipeline failed correctly but substrings %#v are not present in message:\n%v", tc.name, missing, str)
		}
	}
}

func TestAllWithinBounds_BadInts(t *testing.T) {
	var badBoundsTests = []struct {
		name       string
		inputs     []int
		lo         float64
		hi         float64
		errorParts []string
	}{
		{
			"out of int bounds low",
			[]int{-1, 0, 1},
			0,
			1,
			[]string{"values below minimum value 0:", "[-1]"},
		},
		{
			"out of int bounds high",
			[]int{0, 1, 2},
			0,
			1,
			[]string{"values above maximum value 1:", "[2]"},
		},
		{
			"out of int bounds both",
			[]int{-1, 0, 1, 2},
			0,
			1,
			[]string{"values below minimum value 0:", "[-1]", "values above maximum value 1:", "[2]"},
		},
		{
			"out of int bounds in-order",
			[]int{0, 1, 2, 3},
			0,
			1,
			[]string{"values above maximum value 1:", "[2 3]"},
		},
		{
			"out of float bounds low",
			[]int{0, 1, 2},
			0.5,
			2.5,
			[]string{"values below minimum value 0.5:", "[0]"},
		},
		{
			"out of float bounds high",
			[]int{1, 2, 3},
			0.5,
			2.5,
			[]string{"values above maximum value 2.5:", "[3]"},
		},
		{
			"out of float bounds both",
			[]int{0, 1, 2, 3},
			0.5,
			2.5,
			[]string{"values below minimum value 0.5:", "[0]", "values above maximum value 2.5:", "[3]"},
		},
		{
			"out of float bounds in-order",
			[]int{0, -1, 1, 2},
			0.5,
			2.5,
			[]string{"values below minimum value 0.5:", "[-1 0]"},
		},
	}
	for _, tc := range badBoundsTests {
		p, s := beam.NewPipelineWithRoot()
		col := beam.CreateList(s, tc.inputs)
		AllWithinBounds(s, col, tc.lo, tc.hi)
		err := ptest.Run(p)
		if err == nil {
			t.Fatalf("Pipeline succeeded but should have failed.")
		}
		str := err.Error()
		missing := []string{}
		for _, part := range tc.errorParts {
			if !strings.Contains(str, part) {
				missing = append(missing, part)
			}
		}
		if len(missing) != 0 {
			t.Errorf("%v: pipeline failed correctly but substrings %#v are not present in message:\n%v", tc.name, missing, str)
		}
	}
}
