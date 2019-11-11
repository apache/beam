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

	pb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
)

func TestEnsureUniqueName(t *testing.T) {
	tests := []struct {
		in, exp map[string]*pb.PTransform
	}{
		{
			in: map[string]*pb.PTransform{
				"1": {UniqueName: "a"},
				"2": {UniqueName: "b"},
				"3": {UniqueName: "c"},
			},
			exp: map[string]*pb.PTransform{
				"1": {UniqueName: "a"},
				"2": {UniqueName: "b"},
				"3": {UniqueName: "c"},
			},
		},
		{
			in: map[string]*pb.PTransform{
				"2": {UniqueName: "a"},
				"1": {UniqueName: "a"},
				"3": {UniqueName: "a"},
			},
			exp: map[string]*pb.PTransform{
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
		in, exp map[string]*pb.PTransform
	}{
		{ // singleton composite
			in: map[string]*pb.PTransform{
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
			exp: map[string]*pb.PTransform{
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
		{ // closed composite
			in: map[string]*pb.PTransform{
				"1": {
					UniqueName:    "a",
					Subtransforms: []string{"2", "3"},
				},
				"2": {UniqueName: "b", Outputs: map[string]string{"i0": "p1"}},
				"3": {UniqueName: "c", Inputs: map[string]string{"i0": "p1"}},
			},
			exp: map[string]*pb.PTransform{
				"1": {
					UniqueName:    "a",
					Subtransforms: []string{"2", "3"},
				},
				"2": {UniqueName: "b", Outputs: map[string]string{"i0": "p1"}},
				"3": {UniqueName: "c", Inputs: map[string]string{"i0": "p1"}},
			},
		},
	}

	for _, test := range tests {
		actual := computeCompositeInputOutput(test.in)
		if !cmp.Equal(actual, test.exp, cmp.Comparer(proto.Equal)) {
			t.Errorf("coimputeInputOutput(%v) = %v, want %v", test.in, actual, test.exp)
		}
	}
}
