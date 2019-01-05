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

package beam_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
)

func TestCombine(t *testing.T) {
	tests := []struct {
		name      string
		combineFn interface{}
		in        []interface{}
		exp       interface{}
	}{
		{
			name:      "MyCombine",
			combineFn: &MyCombine{},
			in:        []interface{}{1, 2, 3, 4},
			exp:       int(10),
		},
		{
			name:      "MyUniversalCombine", // Test for BEAM-3580.
			combineFn: &MyUniversalCombine{},
			in:        []interface{}{"1", "2", "3", "4", "5"},
			exp:       int(15),
		},
	}

	for _, test := range tests {
		p, s, in := ptest.CreateList(test.in)
		exp := beam.Create(s, test.exp)
		passert.Equals(s, beam.Combine(s, test.combineFn, in), exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("beam.Combine(%v, %v) != %v: %v", test.name, test.in, test.exp, err)
		}
	}
}

// MyCombine represents a combine with the same Input and Output type (int), but a
// distinct accumulator type (int64).
//
//  InputT == OutputT == int
//  AccumT == int64
type MyCombine struct{}

func (*MyCombine) AddInput(a int64, v int) int64 {
	return a + int64(v)
}

func (*MyCombine) MergeAccumulators(a, b int64) int64 {
	return a + b
}

func (*MyCombine) ExtractOutput(a int64) int {
	return int(a)
}

// MyUniversalCombine has universal Input type beam.T, but only accepts
// underlying type string. It doesn't specify an ExtractOutput.
//
//  InputT == beam.T (underlying type string)
//  AccumT == int
//  OutputT == int
type MyUniversalCombine struct{}

func (c *MyUniversalCombine) AddInput(a int, v beam.T) (int, error) {
	s, ok := v.(string)
	if !ok {
		return 0, fmt.Errorf("expecting input type string, got %T", v)
	}

	num, err := strconv.ParseInt(s, 0, 0)
	if err != nil {
		return 0, err
	}
	return c.MergeAccumulators(a, int(num)), nil
}

func (*MyUniversalCombine) MergeAccumulators(a, b int) int {
	return a + b
}
