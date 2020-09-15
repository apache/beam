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

package pipelinex

import (
	"testing"

	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
)

func TestEnsureUniqueName(t *testing.T) {
	tests := []struct {
		in, exp map[string]*pipepb.PTransform
	}{
		{
			in: map[string]*pipepb.PTransform{
				"1": {UniqueName: "a"},
				"2": {UniqueName: "b"},
				"3": {UniqueName: "c"},
			},
			exp: map[string]*pipepb.PTransform{
				"1": {UniqueName: "a"},
				"2": {UniqueName: "b"},
				"3": {UniqueName: "c"},
			},
		},
		{
			in: map[string]*pipepb.PTransform{
				"2": {UniqueName: "a"},
				"1": {UniqueName: "a"},
				"3": {UniqueName: "a"},
			},
			exp: map[string]*pipepb.PTransform{
				"1": {UniqueName: "a"},
				"2": {UniqueName: "a'1"},
				"3": {UniqueName: "a'2"},
			},
		},
	}

	for _, test := range tests {
		actual := ensureUniqueNames(test.in)
		if !cmp.Equal(actual, test.exp, cmp.Comparer(proto.Equal)) {
			t.Errorf("ensureUniqueName(%v) = %v, want %v", test.in, actual, test.exp)
		}
	}
}

func TestComputeInputOutput(t *testing.T) {
	tests := []struct {
		name    string
		in, exp map[string]*pipepb.PTransform
	}{
		{
			name: "singleton composite",
			in: map[string]*pipepb.PTransform{
				"1": {
					UniqueName:    "a",
					Subtransforms: []string{"2"},
				},
				"2": {
					UniqueName: "b",
					Inputs:     map[string]string{"i0": "p1"},
					Outputs:    map[string]string{"i0": "p2"},
				},
			},
			exp: map[string]*pipepb.PTransform{
				"1": {
					UniqueName:    "a",
					Subtransforms: []string{"2"},
					Inputs:        map[string]string{"p1": "p1"},
					Outputs:       map[string]string{"p2": "p2"},
				},
				"2": {
					UniqueName: "b",
					Inputs:     map[string]string{"i0": "p1"},
					Outputs:    map[string]string{"i0": "p2"},
				},
			},
		},
		{
			name: "closed composite",
			in: map[string]*pipepb.PTransform{
				"1": {
					UniqueName:    "a",
					Subtransforms: []string{"2", "3"},
				},
				"2": {UniqueName: "b", Outputs: map[string]string{"i0": "p1"}},
				"3": {UniqueName: "c", Inputs: map[string]string{"i0": "p1"}},
			},
			exp: map[string]*pipepb.PTransform{
				"1": {
					UniqueName:    "a",
					Subtransforms: []string{"2", "3"},
				},
				"2": {UniqueName: "b", Outputs: map[string]string{"i0": "p1"}},
				"3": {UniqueName: "c", Inputs: map[string]string{"i0": "p1"}},
			},
		},
		{
			name: "nested composites",
			in: map[string]*pipepb.PTransform{
				"1": {
					UniqueName:    "a",
					Subtransforms: []string{"2"},
				},
				"2": {
					UniqueName:    "b",
					Subtransforms: []string{"3", "7", "8"},
				},
				"3": {
					UniqueName:    "c",
					Subtransforms: []string{"4", "5", "6"},
				},
				"4": {UniqueName: "d", Inputs: map[string]string{"i0": "p1"}, Outputs: map[string]string{"i0": "p2"}},
				"5": {UniqueName: "e", Inputs: map[string]string{"i0": "p2"}, Outputs: map[string]string{"i0": "p3", "i1": "p4"}},
				"6": {UniqueName: "f", Inputs: map[string]string{"i0": "p2", "i1": "p5"}, Outputs: map[string]string{"i0": "p6"}},
				"7": {UniqueName: "g", Inputs: map[string]string{"i0": "p4", "i1": "p6", "i2": "p8"}, Outputs: map[string]string{"i0": "p7"}},
				"8": {UniqueName: "h", Inputs: map[string]string{"i0": "p7"}},
			},
			exp: map[string]*pipepb.PTransform{
				"1": {
					UniqueName:    "a",
					Subtransforms: []string{"2"},
					Inputs:        map[string]string{"p1": "p1", "p5": "p5", "p8": "p8"},
					Outputs:       map[string]string{"p3": "p3"},
				},
				"2": {
					UniqueName:    "b",
					Subtransforms: []string{"3", "7", "8"},
					Inputs:        map[string]string{"p1": "p1", "p5": "p5", "p8": "p8"},
					Outputs:       map[string]string{"p3": "p3"},
				},
				"3": {
					UniqueName:    "c",
					Subtransforms: []string{"4", "6", "5"}, // topologically sorted.
					Inputs:        map[string]string{"p1": "p1", "p5": "p5"},
					Outputs:       map[string]string{"p4": "p4", "p6": "p6", "p3": "p3"},
				},
				"4": {UniqueName: "d", Inputs: map[string]string{"i0": "p1"}, Outputs: map[string]string{"i0": "p2"}},
				"5": {UniqueName: "e", Inputs: map[string]string{"i0": "p2"}, Outputs: map[string]string{"i0": "p3", "i1": "p4"}},
				"6": {UniqueName: "f", Inputs: map[string]string{"i0": "p2", "i1": "p5"}, Outputs: map[string]string{"i0": "p6"}},
				"7": {UniqueName: "g", Inputs: map[string]string{"i0": "p4", "i1": "p6", "i2": "p8"}, Outputs: map[string]string{"i0": "p7"}},
				"8": {UniqueName: "h", Inputs: map[string]string{"i0": "p7"}},
			},
		}, {
			name: "sibling composite",
			in: map[string]*pipepb.PTransform{
				"1": {
					UniqueName:    "a",
					Subtransforms: []string{"3", "4"},
				},
				"2": {
					UniqueName:    "b",
					Subtransforms: []string{"5"},
				},
				"3": {UniqueName: "c", Outputs: map[string]string{"i0": "p1"}},
				"4": {UniqueName: "d", Inputs: map[string]string{"i0": "p1"}},
				"5": {UniqueName: "e", Inputs: map[string]string{"i0": "p1"}},
			},
			exp: map[string]*pipepb.PTransform{
				"1": {
					UniqueName:    "a",
					Subtransforms: []string{"3", "4"},
					Outputs:       map[string]string{"p1": "p1"},
				},
				"2": {
					UniqueName:    "b",
					Subtransforms: []string{"5"},
					Inputs:        map[string]string{"p1": "p1"},
				},
				"3": {UniqueName: "c", Outputs: map[string]string{"i0": "p1"}},
				"4": {UniqueName: "d", Inputs: map[string]string{"i0": "p1"}},
				"5": {UniqueName: "e", Inputs: map[string]string{"i0": "p1"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := computeCompositeInputOutput(test.in)
			if diff := cmp.Diff(actual, test.exp, cmp.Comparer(proto.Equal)); diff != "" {
				t.Errorf("computeInputOutput(%v)\ndiff: %v", test.in, diff)
			}
		})
	}
}
