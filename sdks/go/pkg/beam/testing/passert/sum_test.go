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

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestSum_good(t *testing.T) {
	tests := []struct {
		name   string
		values []int
		size   int
		total  int
	}{
		{
			"all positive",
			[]int{1, 2, 3, 4, 5},
			5,
			15,
		},
		{
			"all negative",
			[]int{-1, -2, -3, -4, -5},
			5,
			-15,
		},
		{
			"mixed",
			[]int{1, -2, 3, -4, 5},
			5,
			3,
		},
		{
			"empty",
			[]int{},
			0,
			0,
		},
	}
	for _, tc := range tests {
		p, s := beam.NewPipelineWithRoot()
		col := beam.CreateList(s, tc.values)
		Sum(s, col, tc.name, tc.size, tc.total)
		if err := ptest.Run(p); err != nil {
			t.Errorf("Pipeline failed: %v", err)
		}
	}
}

func TestSum_bad(t *testing.T) {
	tests := []struct {
		name       string
		col        []int
		size       int
		total      int
		errorParts []string
	}{
		{
			"bad size",
			[]int{1, 2, 3, 4, 5},
			4,
			15,
			[]string{"{15, size: 5}", "want {15, size:4}"},
		},
		{
			"bad total",
			[]int{1, 2, 3, 4, 5},
			5,
			16,
			[]string{"{15, size: 5}", "want {16, size:5}"},
		},
		{
			"empty",
			[]int{},
			1,
			1,
			[]string{"PCollection is empty, want non-empty collection"},
		},
	}
	for _, tc := range tests {
		p, s := beam.NewPipelineWithRoot()
		col := beam.CreateList(s, tc.col)
		Sum(s, col, tc.name, tc.size, tc.total)
		err := ptest.Run(p)
		if err == nil {
			t.Fatalf("Pipeline succeeded but should have failed: %v", tc.name)
		}
		str := err.Error()
		missing := []string{}
		for _, part := range tc.errorParts {
			if !strings.Contains(str, part) {
				missing = append(missing, part)
			}
		}
		if len(missing) != 0 {
			t.Errorf("%v: pipeline failed correctly, but substrings %#v are not present in message:\n%v", tc.name, missing, str)
		}
	}
}
