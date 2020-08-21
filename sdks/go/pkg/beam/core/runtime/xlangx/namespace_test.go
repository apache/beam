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

package xlangx

import (
	"strings"
	"testing"

	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func assertPanic(t *testing.T, f func(), err string) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic; %v", err)
		}
	}()
	f()
}

func TestAddNamespace(t *testing.T) {
	tests := []struct {
		name        string
		init        *pipepb.Components
		namespace   string
		transformID string
		want        *pipepb.Components
		err         string
	}{
		{
			name: "[Correctness] Single Input Multi Output",
			init: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"t0": &pipepb.PTransform{
						UniqueName:    "t0",
						Inputs:        map[string]string{"t0i0": "p0"},
						Outputs:       map[string]string{"t0o0": "p1", "t0o1": "p2"},
						EnvironmentId: "e0",
					},
					"t1": &pipepb.PTransform{
						UniqueName:    "t1",
						Inputs:        map[string]string{"t1i0": "p1"},
						Outputs:       map[string]string{"t1o0": "p3"},
						EnvironmentId: "e1",
					},
					"t2": &pipepb.PTransform{
						UniqueName:    "t2",
						Inputs:        map[string]string{"t2i0": "p2"},
						Outputs:       map[string]string{"t2o0": "p4"},
						EnvironmentId: "e0",
					},
					"t3": &pipepb.PTransform{
						UniqueName:    "t3",
						Inputs:        map[string]string{"t3i0": "p3", "t3i1": "p4"},
						Outputs:       map[string]string{"t3o0": "p5"},
						EnvironmentId: "e1",
					},
				},
				Pcollections: map[string]*pipepb.PCollection{
					"p0": &pipepb.PCollection{CoderId: "c0", WindowingStrategyId: "w1"},
					"p1": &pipepb.PCollection{CoderId: "c1", WindowingStrategyId: "w1"},
					"p2": &pipepb.PCollection{CoderId: "c0", WindowingStrategyId: "w1"},
					"p3": &pipepb.PCollection{CoderId: "c3", WindowingStrategyId: "w1"},
					"p4": &pipepb.PCollection{CoderId: "c2", WindowingStrategyId: "w0"},
					"p5": &pipepb.PCollection{CoderId: "c2", WindowingStrategyId: "w1"},
				},
				WindowingStrategies: map[string]*pipepb.WindowingStrategy{
					"w0": &pipepb.WindowingStrategy{WindowCoderId: "c3", EnvironmentId: "e0"},
					"w1": &pipepb.WindowingStrategy{WindowCoderId: "c4", EnvironmentId: "e1"},
				},
				Coders: map[string]*pipepb.Coder{
					"c0": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c0"}},
					"c1": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c1"}},
					"c2": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c2"}},
					"c3": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c3"}},
					"c4": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c4"}},
				},
				Environments: map[string]*pipepb.Environment{
					"e0": &pipepb.Environment{Urn: "e0"},
					"e1": &pipepb.Environment{Urn: "e1"},
				},
			},
			namespace:   "daASxQwenJ",
			transformID: "t0",
			want: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"t0": &pipepb.PTransform{
						UniqueName:    "t0",
						Inputs:        map[string]string{"t0i0": "p0"},
						Outputs:       map[string]string{"t0o0": "p1", "t0o1": "p2"},
						EnvironmentId: "e0@daASxQwenJ",
					},
					"t1": &pipepb.PTransform{
						UniqueName:    "t1",
						Inputs:        map[string]string{"t1i0": "p1"},
						Outputs:       map[string]string{"t1o0": "p3"},
						EnvironmentId: "e1@daASxQwenJ",
					},
					"t2": &pipepb.PTransform{
						UniqueName:    "t2",
						Inputs:        map[string]string{"t2i0": "p2"},
						Outputs:       map[string]string{"t2o0": "p4"},
						EnvironmentId: "e0@daASxQwenJ",
					},
					"t3": &pipepb.PTransform{
						UniqueName:    "t3",
						Inputs:        map[string]string{"t3i0": "p3", "t3i1": "p4"},
						Outputs:       map[string]string{"t3o0": "p5"},
						EnvironmentId: "e1@daASxQwenJ",
					},
				},
				Pcollections: map[string]*pipepb.PCollection{
					"p0": &pipepb.PCollection{CoderId: "c0@daASxQwenJ", WindowingStrategyId: "w1@daASxQwenJ"},
					"p1": &pipepb.PCollection{CoderId: "c1@daASxQwenJ", WindowingStrategyId: "w1@daASxQwenJ"},
					"p2": &pipepb.PCollection{CoderId: "c0@daASxQwenJ", WindowingStrategyId: "w1@daASxQwenJ"},
					"p3": &pipepb.PCollection{CoderId: "c3", WindowingStrategyId: "w1@daASxQwenJ"},
					"p4": &pipepb.PCollection{CoderId: "c2", WindowingStrategyId: "w0"},
					"p5": &pipepb.PCollection{CoderId: "c2", WindowingStrategyId: "w1@daASxQwenJ"},
				},
				WindowingStrategies: map[string]*pipepb.WindowingStrategy{
					"w0":            &pipepb.WindowingStrategy{WindowCoderId: "c3", EnvironmentId: "e0"},
					"w1@daASxQwenJ": &pipepb.WindowingStrategy{WindowCoderId: "c4@daASxQwenJ", EnvironmentId: "e1@daASxQwenJ"},
				},
				Coders: map[string]*pipepb.Coder{
					"c0@daASxQwenJ": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c0"}},
					"c1@daASxQwenJ": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c1"}},
					"c2":            &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c2"}},
					"c3":            &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c3"}},
					"c4@daASxQwenJ": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c4"}},
				},
				Environments: map[string]*pipepb.Environment{
					"e0@daASxQwenJ": &pipepb.Environment{Urn: "e0"},
					"e1@daASxQwenJ": &pipepb.Environment{Urn: "e1"},
				},
			},
		},
		{
			name: "[Correctness] Single Input Single Output",
			init: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"t0": &pipepb.PTransform{
						UniqueName:    "t0",
						Inputs:        map[string]string{"t0i0": "p0"},
						Outputs:       map[string]string{"t0o0": "p1", "t0o1": "p2"},
						EnvironmentId: "e0",
					},
					"t1": &pipepb.PTransform{
						UniqueName:    "t1",
						Inputs:        map[string]string{"t1i0": "p1"},
						Outputs:       map[string]string{"t1o0": "p3"},
						EnvironmentId: "e1",
					},
					"t2": &pipepb.PTransform{
						UniqueName:    "t2",
						Inputs:        map[string]string{"t2i0": "p2"},
						Outputs:       map[string]string{"t2o0": "p4"},
						EnvironmentId: "e0",
					},
					"t3": &pipepb.PTransform{
						UniqueName:    "t3",
						Inputs:        map[string]string{"t3i0": "p3", "t3i1": "p4"},
						Outputs:       map[string]string{"t3o0": "p5"},
						EnvironmentId: "e1",
					},
				},
				Pcollections: map[string]*pipepb.PCollection{
					"p0": &pipepb.PCollection{CoderId: "c0", WindowingStrategyId: "w1"},
					"p1": &pipepb.PCollection{CoderId: "c1", WindowingStrategyId: "w1"},
					"p2": &pipepb.PCollection{CoderId: "c0", WindowingStrategyId: "w1"},
					"p3": &pipepb.PCollection{CoderId: "c3", WindowingStrategyId: "w1"},
					"p4": &pipepb.PCollection{CoderId: "c2", WindowingStrategyId: "w0"},
					"p5": &pipepb.PCollection{CoderId: "c2", WindowingStrategyId: "w1"},
				},
				WindowingStrategies: map[string]*pipepb.WindowingStrategy{
					"w0": &pipepb.WindowingStrategy{WindowCoderId: "c3", EnvironmentId: "e0"},
					"w1": &pipepb.WindowingStrategy{WindowCoderId: "c4", EnvironmentId: "e1"},
				},
				Coders: map[string]*pipepb.Coder{
					"c0": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c0"}},
					"c1": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c1"}},
					"c2": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c2"}},
					"c3": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c3"}},
					"c4": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c4"}},
				},
				Environments: map[string]*pipepb.Environment{
					"e0": &pipepb.Environment{Urn: "e0"},
					"e1": &pipepb.Environment{Urn: "e1"},
				},
			},
			namespace:   "daASxQwenJ",
			transformID: "t1",
			want: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"t0": &pipepb.PTransform{
						UniqueName:    "t0",
						Inputs:        map[string]string{"t0i0": "p0"},
						Outputs:       map[string]string{"t0o0": "p1", "t0o1": "p2"},
						EnvironmentId: "e0",
					},
					"t1": &pipepb.PTransform{
						UniqueName:    "t1",
						Inputs:        map[string]string{"t1i0": "p1"},
						Outputs:       map[string]string{"t1o0": "p3"},
						EnvironmentId: "e1@daASxQwenJ",
					},
					"t2": &pipepb.PTransform{
						UniqueName:    "t2",
						Inputs:        map[string]string{"t2i0": "p2"},
						Outputs:       map[string]string{"t2o0": "p4"},
						EnvironmentId: "e0",
					},
					"t3": &pipepb.PTransform{
						UniqueName:    "t3",
						Inputs:        map[string]string{"t3i0": "p3", "t3i1": "p4"},
						Outputs:       map[string]string{"t3o0": "p5"},
						EnvironmentId: "e1@daASxQwenJ",
					},
				},
				Pcollections: map[string]*pipepb.PCollection{
					"p0": &pipepb.PCollection{CoderId: "c0", WindowingStrategyId: "w1@daASxQwenJ"},
					"p1": &pipepb.PCollection{CoderId: "c1@daASxQwenJ", WindowingStrategyId: "w1@daASxQwenJ"},
					"p2": &pipepb.PCollection{CoderId: "c0", WindowingStrategyId: "w1@daASxQwenJ"},
					"p3": &pipepb.PCollection{CoderId: "c3@daASxQwenJ", WindowingStrategyId: "w1@daASxQwenJ"},
					"p4": &pipepb.PCollection{CoderId: "c2", WindowingStrategyId: "w0"},
					"p5": &pipepb.PCollection{CoderId: "c2", WindowingStrategyId: "w1@daASxQwenJ"},
				},
				WindowingStrategies: map[string]*pipepb.WindowingStrategy{
					"w0":            &pipepb.WindowingStrategy{WindowCoderId: "c3", EnvironmentId: "e0"},
					"w1@daASxQwenJ": &pipepb.WindowingStrategy{WindowCoderId: "c4@daASxQwenJ", EnvironmentId: "e1@daASxQwenJ"},
				},
				Coders: map[string]*pipepb.Coder{
					"c0":            &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c0"}},
					"c1@daASxQwenJ": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c1"}},
					"c2":            &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c2"}},
					"c3@daASxQwenJ": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c3"}},
					"c4@daASxQwenJ": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c4"}},
				},
				Environments: map[string]*pipepb.Environment{
					"e0":            &pipepb.Environment{Urn: "e0"},
					"e1@daASxQwenJ": &pipepb.Environment{Urn: "e1"},
				},
			},
		},
		{
			name: "[Correctness] Multi Input Single Output",
			init: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"t0": &pipepb.PTransform{
						UniqueName:    "t0",
						Inputs:        map[string]string{"t0i0": "p0"},
						Outputs:       map[string]string{"t0o0": "p1", "t0o1": "p2"},
						EnvironmentId: "e0",
					},
					"t1": &pipepb.PTransform{
						UniqueName:    "t1",
						Inputs:        map[string]string{"t1i0": "p1"},
						Outputs:       map[string]string{"t1o0": "p3"},
						EnvironmentId: "e1",
					},
					"t2": &pipepb.PTransform{
						UniqueName:    "t2",
						Inputs:        map[string]string{"t2i0": "p2"},
						Outputs:       map[string]string{"t2o0": "p4"},
						EnvironmentId: "e0",
					},
					"t3": &pipepb.PTransform{
						UniqueName:    "t3",
						Inputs:        map[string]string{"t3i0": "p3", "t3i1": "p4"},
						Outputs:       map[string]string{"t3o0": "p5"},
						EnvironmentId: "e1",
					},
				},
				Pcollections: map[string]*pipepb.PCollection{
					"p0": &pipepb.PCollection{CoderId: "c0", WindowingStrategyId: "w1"},
					"p1": &pipepb.PCollection{CoderId: "c1", WindowingStrategyId: "w1"},
					"p2": &pipepb.PCollection{CoderId: "c0", WindowingStrategyId: "w1"},
					"p3": &pipepb.PCollection{CoderId: "c3", WindowingStrategyId: "w1"},
					"p4": &pipepb.PCollection{CoderId: "c2", WindowingStrategyId: "w0"},
					"p5": &pipepb.PCollection{CoderId: "c2", WindowingStrategyId: "w1"},
				},
				WindowingStrategies: map[string]*pipepb.WindowingStrategy{
					"w0": &pipepb.WindowingStrategy{WindowCoderId: "c3", EnvironmentId: "e0"},
					"w1": &pipepb.WindowingStrategy{WindowCoderId: "c4", EnvironmentId: "e1"},
				},
				Coders: map[string]*pipepb.Coder{
					"c0": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c0"}},
					"c1": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c1"}},
					"c2": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c2"}},
					"c3": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c3"}},
					"c4": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c4"}},
				},
				Environments: map[string]*pipepb.Environment{
					"e0": &pipepb.Environment{Urn: "e0"},
					"e1": &pipepb.Environment{Urn: "e1"},
				},
			},
			namespace:   "daASxQwenJ",
			transformID: "t3",
			want: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"t0": &pipepb.PTransform{
						UniqueName:    "t0",
						Inputs:        map[string]string{"t0i0": "p0"},
						Outputs:       map[string]string{"t0o0": "p1", "t0o1": "p2"},
						EnvironmentId: "e0@daASxQwenJ",
					},
					"t1": &pipepb.PTransform{
						UniqueName:    "t1",
						Inputs:        map[string]string{"t1i0": "p1"},
						Outputs:       map[string]string{"t1o0": "p3"},
						EnvironmentId: "e1@daASxQwenJ",
					},
					"t2": &pipepb.PTransform{
						UniqueName:    "t2",
						Inputs:        map[string]string{"t2i0": "p2"},
						Outputs:       map[string]string{"t2o0": "p4"},
						EnvironmentId: "e0@daASxQwenJ",
					},
					"t3": &pipepb.PTransform{
						UniqueName:    "t3",
						Inputs:        map[string]string{"t3i0": "p3", "t3i1": "p4"},
						Outputs:       map[string]string{"t3o0": "p5"},
						EnvironmentId: "e1@daASxQwenJ",
					},
				},
				Pcollections: map[string]*pipepb.PCollection{
					"p0": &pipepb.PCollection{CoderId: "c0", WindowingStrategyId: "w1@daASxQwenJ"},
					"p1": &pipepb.PCollection{CoderId: "c1", WindowingStrategyId: "w1@daASxQwenJ"},
					"p2": &pipepb.PCollection{CoderId: "c0", WindowingStrategyId: "w1@daASxQwenJ"},
					"p3": &pipepb.PCollection{CoderId: "c3@daASxQwenJ", WindowingStrategyId: "w1@daASxQwenJ"},
					"p4": &pipepb.PCollection{CoderId: "c2@daASxQwenJ", WindowingStrategyId: "w0@daASxQwenJ"},
					"p5": &pipepb.PCollection{CoderId: "c2@daASxQwenJ", WindowingStrategyId: "w1@daASxQwenJ"},
				},
				WindowingStrategies: map[string]*pipepb.WindowingStrategy{
					"w0@daASxQwenJ": &pipepb.WindowingStrategy{WindowCoderId: "c3@daASxQwenJ", EnvironmentId: "e0@daASxQwenJ"},
					"w1@daASxQwenJ": &pipepb.WindowingStrategy{WindowCoderId: "c4@daASxQwenJ", EnvironmentId: "e1@daASxQwenJ"},
				},
				Coders: map[string]*pipepb.Coder{
					"c0":            &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c0"}},
					"c1":            &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c1"}},
					"c2@daASxQwenJ": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c2"}},
					"c3@daASxQwenJ": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c3"}},
					"c4@daASxQwenJ": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c4"}},
				},
				Environments: map[string]*pipepb.Environment{
					"e0@daASxQwenJ": &pipepb.Environment{Urn: "e0"},
					"e1@daASxQwenJ": &pipepb.Environment{Urn: "e1"},
				},
			},
		},
		{
			name: "[Correctness] Component Coders",
			init: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"t0": &pipepb.PTransform{
						UniqueName:    "t0",
						Inputs:        map[string]string{"t0i0": "p0"},
						Outputs:       map[string]string{"t0o0": "p1"},
						EnvironmentId: "e0",
					},
					"t1": &pipepb.PTransform{
						UniqueName:    "t1",
						Inputs:        map[string]string{"t1i0": "p1"},
						Outputs:       map[string]string{"t1o0": "p2"},
						EnvironmentId: "e1",
					},
				},
				Pcollections: map[string]*pipepb.PCollection{
					"p0": &pipepb.PCollection{CoderId: "c0", WindowingStrategyId: "w0"},
					"p1": &pipepb.PCollection{CoderId: "c1", WindowingStrategyId: "w1"},
					"p2": &pipepb.PCollection{CoderId: "c0", WindowingStrategyId: "w1"},
				},
				WindowingStrategies: map[string]*pipepb.WindowingStrategy{
					"w0": &pipepb.WindowingStrategy{WindowCoderId: "c3", EnvironmentId: "e0"},
					"w1": &pipepb.WindowingStrategy{WindowCoderId: "c4", EnvironmentId: "e1"},
				},
				Coders: map[string]*pipepb.Coder{
					"c0": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c0"}, ComponentCoderIds: []string{"c2"}},
					"c1": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c1"}},
					"c2": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c2"}},
					"c3": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c3"}},
					"c4": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c4"}},
				},
				Environments: map[string]*pipepb.Environment{
					"e0": &pipepb.Environment{Urn: "e0"},
					"e1": &pipepb.Environment{Urn: "e1"},
				},
			},
			namespace:   "daASxQwenJ",
			transformID: "t0",
			want: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"t0": &pipepb.PTransform{
						UniqueName:    "t0",
						Inputs:        map[string]string{"t0i0": "p0"},
						Outputs:       map[string]string{"t0o0": "p1"},
						EnvironmentId: "e0@daASxQwenJ",
					},
					"t1": &pipepb.PTransform{
						UniqueName:    "t1",
						Inputs:        map[string]string{"t1i0": "p1"},
						Outputs:       map[string]string{"t1o0": "p2"},
						EnvironmentId: "e1@daASxQwenJ",
					},
				},
				Pcollections: map[string]*pipepb.PCollection{
					"p0": &pipepb.PCollection{CoderId: "c0@daASxQwenJ", WindowingStrategyId: "w0@daASxQwenJ"},
					"p1": &pipepb.PCollection{CoderId: "c1@daASxQwenJ", WindowingStrategyId: "w1@daASxQwenJ"},
					"p2": &pipepb.PCollection{CoderId: "c0@daASxQwenJ", WindowingStrategyId: "w1@daASxQwenJ"},
				},
				WindowingStrategies: map[string]*pipepb.WindowingStrategy{
					"w0@daASxQwenJ": &pipepb.WindowingStrategy{WindowCoderId: "c3@daASxQwenJ", EnvironmentId: "e0@daASxQwenJ"},
					"w1@daASxQwenJ": &pipepb.WindowingStrategy{WindowCoderId: "c4@daASxQwenJ", EnvironmentId: "e1@daASxQwenJ"},
				},
				Coders: map[string]*pipepb.Coder{
					"c0@daASxQwenJ": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c0"}, ComponentCoderIds: []string{"c2@daASxQwenJ"}},
					"c1@daASxQwenJ": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c1"}},
					"c2@daASxQwenJ": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c2"}},
					"c3@daASxQwenJ": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c3"}},
					"c4@daASxQwenJ": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c4"}},
				},
				Environments: map[string]*pipepb.Environment{
					"e0@daASxQwenJ": &pipepb.Environment{Urn: "e0"},
					"e1@daASxQwenJ": &pipepb.Environment{Urn: "e1"},
				},
			},
		},
		{
			name: "[Consistency] Missing EnvironmentID",
			init: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"t0": &pipepb.PTransform{
						UniqueName:    "t0",
						Inputs:        map[string]string{"t0i0": "p0"},
						Outputs:       map[string]string{"t0o0": "p1"},
						EnvironmentId: "e0",
					},
					"t1": &pipepb.PTransform{
						UniqueName:    "t1",
						Inputs:        map[string]string{"t1i0": "p1"},
						Outputs:       map[string]string{"t1o0": "p2"},
						EnvironmentId: "e1",
					},
				},
				Pcollections: map[string]*pipepb.PCollection{
					"p0": &pipepb.PCollection{CoderId: "c0", WindowingStrategyId: "w0"},
					"p1": &pipepb.PCollection{CoderId: "c1", WindowingStrategyId: "w1"},
					"p2": &pipepb.PCollection{CoderId: "c0", WindowingStrategyId: "w1"},
				},
				WindowingStrategies: map[string]*pipepb.WindowingStrategy{
					"w0": &pipepb.WindowingStrategy{WindowCoderId: "c3", EnvironmentId: "e0"},
					"w1": &pipepb.WindowingStrategy{WindowCoderId: "c4", EnvironmentId: "e1"},
				},
				Coders: map[string]*pipepb.Coder{
					"c0": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c0"}},
					"c1": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c1"}},
				},
				Environments: map[string]*pipepb.Environment{
					// Missing "e0"
					"e1": &pipepb.Environment{Urn: "e1"},
				},
			},
			namespace:   "daASxQwenJ",
			transformID: "t0",
			err:         "trying to add an Environment whose key is absent should panic",
		},
		{
			name: "[Consistency] Missing WindowingStrategyID",
			init: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"t0": &pipepb.PTransform{
						UniqueName:    "t0",
						Inputs:        map[string]string{"t0i0": "p0"},
						Outputs:       map[string]string{"t0o0": "p1"},
						EnvironmentId: "e0",
					},
					"t1": &pipepb.PTransform{
						UniqueName:    "t1",
						Inputs:        map[string]string{"t1i0": "p1"},
						Outputs:       map[string]string{"t1o0": "p2"},
						EnvironmentId: "e1",
					},
				},
				Pcollections: map[string]*pipepb.PCollection{
					"p0": &pipepb.PCollection{CoderId: "c0", WindowingStrategyId: "w0"},
					"p1": &pipepb.PCollection{CoderId: "c1", WindowingStrategyId: "w1"},
					"p2": &pipepb.PCollection{CoderId: "c0", WindowingStrategyId: "w1"},
				},
				WindowingStrategies: map[string]*pipepb.WindowingStrategy{
					// Missing w0
					"w1": &pipepb.WindowingStrategy{WindowCoderId: "c4", EnvironmentId: "e1"},
				},
				Coders: map[string]*pipepb.Coder{
					"c0": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c0"}},
					"c1": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c1"}},
				},
				Environments: map[string]*pipepb.Environment{
					"e0": &pipepb.Environment{Urn: "e0"},
					"e1": &pipepb.Environment{Urn: "e1"},
				},
			},
			namespace:   "daASxQwenJ",
			transformID: "t0",
			err:         "trying to add an WindowingStrategy whose key is absent should panic",
		},
		{
			name: "[Consistency] Missing CoderID",
			init: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"t0": &pipepb.PTransform{
						UniqueName:    "t0",
						Inputs:        map[string]string{"t0i0": "p0"},
						Outputs:       map[string]string{"t0o0": "p1"},
						EnvironmentId: "e0",
					},
					"t1": &pipepb.PTransform{
						UniqueName:    "t1",
						Inputs:        map[string]string{"t1i0": "p1"},
						Outputs:       map[string]string{"t1o0": "p2"},
						EnvironmentId: "e1",
					},
				},
				Pcollections: map[string]*pipepb.PCollection{
					"p0": &pipepb.PCollection{CoderId: "c0", WindowingStrategyId: "w0"},
					"p1": &pipepb.PCollection{CoderId: "c1", WindowingStrategyId: "w1"},
					"p2": &pipepb.PCollection{CoderId: "c0", WindowingStrategyId: "w1"},
				},
				WindowingStrategies: map[string]*pipepb.WindowingStrategy{
					"w0": &pipepb.WindowingStrategy{WindowCoderId: "c3", EnvironmentId: "e0"},
					"w1": &pipepb.WindowingStrategy{WindowCoderId: "c4", EnvironmentId: "e1"},
				},
				Coders: map[string]*pipepb.Coder{
					// Missing c0
					"c1": &pipepb.Coder{Spec: &pipepb.FunctionSpec{Urn: "c1"}},
				},
				Environments: map[string]*pipepb.Environment{
					"e0": &pipepb.Environment{Urn: "e0"},
					"e1": &pipepb.Environment{Urn: "e1"},
				},
			},
			namespace:   "daASxQwenJ",
			transformID: "t0",
			err:         "trying to add an WindowingStrategy whose key is absent should panic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if strings.Contains(tt.name, "Correctness") {
				transform := tt.init.Transforms[tt.transformID]

				AddNamespace(transform, tt.init, tt.namespace)

				if d := cmp.Diff(tt.want, tt.init, protocmp.Transform()); d != "" {
					t.Errorf("diff (-want, +got): %v", d)
				}
			}

			if strings.Contains(tt.name, "Consistency") {
				transform := tt.init.Transforms[tt.transformID]
				testPanic := func() {
					AddNamespace(transform, tt.init, tt.namespace)
				}
				assertPanic(t, testPanic, tt.err)
			}

		})
	}
}
