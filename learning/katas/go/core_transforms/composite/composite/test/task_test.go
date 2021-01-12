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

package test

import (
	"beam.apache.org/learning/katas/core_transforms/composite/composite/pkg/common"
	"beam.apache.org/learning/katas/core_transforms/composite/composite/pkg/task"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
	"testing"
)

const (
	wantAugmentedScopeLabel = "CountCharacters"
)

type testCase struct {
	input beam.PCollection
	want map[string]int
}

func TestApplyTransform(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	tests := []testCase{
		{
			input: common.CreateLines(s),
			want: map[string]int{
				"a": 7,
				"c": 4,
				"d": 5,
				"e": 14,
				"f": 2,
				"g": 3,
				"h": 1,
				"i": 8,
				"l": 2,
				"m": 4,
				"n": 8,
				"o": 6,
				"p": 6,
				"r": 4,
				"s": 5,
				"t": 3,
				"u": 3,
				"x": 1,
				"A": 1,
				"B": 1,
			},
		},
	}
	for _, tt := range tests {
		gotKV := task.ApplyTransform(s, tt.input)
		if !hasExpectedAugmentedScope(p) {
			t.Errorf("no augmented scope with label %s", wantAugmentedScopeLabel)
		}

		beam.ParDo(s, func(gotCharacter string, got int, emit func(bool)){
			s = s.Scope("TestApplyTransform")
			want := tt.want[gotCharacter]
			if got != want {
				t.Errorf("%s = %v, want %v", gotCharacter, got, want)
			}
		}, gotKV)

		if err := ptest.Run(p); err != nil {
			t.Error(err)
		}
	}
}

func hasExpectedAugmentedScope(p *beam.Pipeline) bool {
	edges, _, _ := p.Build()
	hasAugmentedScope := false
	for _, k := range edges {
		hasAugmentedScope = k.Scope().Label == wantAugmentedScopeLabel
		if hasAugmentedScope {
			return true
		}
	}
	return false
}

