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

package internal

import (
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/protox"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/testing/protocmp"
)

func Test_preprocessor_preProcessGraph(t *testing.T) {
	tests := []struct {
		name  string
		input *pipepb.Components

		wantComponents *pipepb.Components
		wantStages     []*stage
		forcedRoots    []string
	}{
		{
			name: "noPreparer",
			input: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"e1": {
						UniqueName: "e1",
						Spec: &pipepb.FunctionSpec{
							Urn: "defaultUrn",
						},
					},
				},
			},

			wantStages: []*stage{{transforms: []string{"e1"}}},
			wantComponents: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"e1": {
						UniqueName: "e1",
						Spec: &pipepb.FunctionSpec{
							Urn: "defaultUrn",
						},
					},
				},
			},
		}, {
			name:        "preparer",
			forcedRoots: []string{"e1_early", "e1_late"},
			input: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"e1": {
						UniqueName: "e1",
						Spec: &pipepb.FunctionSpec{
							Urn: "test_urn",
						},
					},
				},
				// Initialize maps because they always are by proto unmarshallers.
				Pcollections:        map[string]*pipepb.PCollection{},
				WindowingStrategies: map[string]*pipepb.WindowingStrategy{},
				Coders:              map[string]*pipepb.Coder{},
				Environments:        map[string]*pipepb.Environment{},
			},

			wantStages: []*stage{
				{transforms: []string{"e1_early"}, envID: "env1",
					outputs: []link{{Transform: "e1_early", Local: "i0", Global: "pcol1"}}},
				{transforms: []string{"e1_late"}, envID: "env1", primaryInput: "pcol1"}},
			wantComponents: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					// Original is always kept
					"e1": {
						UniqueName: "e1",
						Spec: &pipepb.FunctionSpec{
							Urn: "test_urn",
						},
					},
					"e1_early": {
						UniqueName: "e1_early",
						Spec: &pipepb.FunctionSpec{
							Urn: "defaultUrn",
						},
						Outputs:       map[string]string{"i0": "pcol1"},
						EnvironmentId: "env1",
					},
					"e1_late": {
						UniqueName: "e1_late",
						Spec: &pipepb.FunctionSpec{
							Urn: "defaultUrn",
						},
						Inputs:        map[string]string{"i0": "pcol1"},
						EnvironmentId: "env1",
					},
				},
				Pcollections: map[string]*pipepb.PCollection{
					"pcol1": {
						UniqueName:          "pcol1",
						CoderId:             "coder1",
						WindowingStrategyId: "ws1",
					},
				},
				Coders: map[string]*pipepb.Coder{
					"coder1": {Spec: &pipepb.FunctionSpec{Urn: "coder1"}},
				},
				WindowingStrategies: map[string]*pipepb.WindowingStrategy{
					"ws1": {WindowCoderId: "global"},
				},
				Environments: map[string]*pipepb.Environment{
					"env1": {Urn: "env1"},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pre := newPreprocessor([]transformPreparer{&testPreparer{
				ForcedRoots: test.forcedRoots,
			}})

			gotStages := pre.preProcessGraph(test.input, nil)
			if diff := cmp.Diff(test.wantStages, gotStages, cmp.AllowUnexported(stage{}, link{}), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("preProcessGraph(%q) stages diff (-want,+got)\n%v", test.name, diff)
			}

			if diff := cmp.Diff(test.wantComponents, test.input, protocmp.Transform()); diff != "" {
				t.Errorf("preProcessGraph(%q) components diff (-want,+got)\n%v", test.name, diff)
			}
		})
	}
}

type testPreparer struct {
	ForcedRoots []string
}

func (p *testPreparer) PrepareUrns() []string {
	return []string{"test_urn"}
}

func (p *testPreparer) PrepareTransform(tid string, t *pipepb.PTransform, comps *pipepb.Components) prepareResult {
	return prepareResult{
		ForcedRoots: p.ForcedRoots,
		SubbedComps: &pipepb.Components{
			Transforms: map[string]*pipepb.PTransform{
				"e1_early": {
					UniqueName: "e1_early",
					Spec: &pipepb.FunctionSpec{
						Urn: "defaultUrn",
					},
					Outputs:       map[string]string{"i0": "pcol1"},
					EnvironmentId: "env1",
				},
				"e1_late": {
					UniqueName: "e1_late",
					Spec: &pipepb.FunctionSpec{
						Urn: "defaultUrn",
					},
					Inputs:        map[string]string{"i0": "pcol1"},
					EnvironmentId: "env1",
				},
			},
			Pcollections: map[string]*pipepb.PCollection{
				"pcol1": {
					UniqueName:          "pcol1",
					CoderId:             "coder1",
					WindowingStrategyId: "ws1",
				},
			},
			Coders: map[string]*pipepb.Coder{
				"coder1": {Spec: &pipepb.FunctionSpec{Urn: "coder1"}},
			},
			WindowingStrategies: map[string]*pipepb.WindowingStrategy{
				"ws1": {WindowCoderId: "global"},
			},
			Environments: map[string]*pipepb.Environment{
				"env1": {Urn: "env1"},
			},
		},
		RemovedLeaves: []string{"e1"},
	}
}

func TestComputeFacts(t *testing.T) {
	sideInputSpec := func(sis map[string]*pipepb.SideInput) *pipepb.FunctionSpec {
		return &pipepb.FunctionSpec{
			Urn: urns.TransformParDo,
			Payload: protox.MustEncode(&pipepb.ParDoPayload{
				SideInputs: sis,
			}),
		}
	}

	tests := []struct {
		name        string
		topological []string
		comps       *pipepb.Components

		want *fusionFacts
	}{
		{
			name:        "single_transform",
			topological: []string{"t1"},
			comps: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"t1": {
						Inputs:  map[string]string{},
						Outputs: map[string]string{},
					},
				},
			},
			want: &fusionFacts{
				PcolProducers:   map[string]link{},
				PcolConsumers:   map[string][]link{},
				UsedAsSideInput: map[string]bool{},
				DirectSideInputs: map[string]map[string]bool{
					"t1": nil,
				},
				DownstreamSideInputs: map[string]map[string]bool{
					"t1": nil,
				},
			},
		}, {
			name:        "t2_consumes_n1_as_primary",
			topological: []string{"t1", "t2"},
			comps: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"t1": {
						Inputs:  map[string]string{},
						Outputs: map[string]string{"o0": "n1"},
					},
					"t2": {
						Inputs:  map[string]string{"i0": "n1"},
						Outputs: map[string]string{},
					},
				},
			},
			want: &fusionFacts{
				PcolProducers: map[string]link{
					"n1": {Transform: "t1", Local: "o0", Global: "n1"},
				},
				PcolConsumers: map[string][]link{
					"n1": {{Transform: "t2", Local: "i0", Global: "n1"}},
				},
				UsedAsSideInput: map[string]bool{},
				DirectSideInputs: map[string]map[string]bool{
					"t1": nil,
					"t2": nil,
				},
				DownstreamSideInputs: map[string]map[string]bool{
					"t1": nil,
					"t2": nil,
				},
			},
		}, {
			name:        "t2_consumes_n1_as_side",
			topological: []string{"t1", "t2"},
			comps: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"t1": {
						Inputs:  map[string]string{},
						Outputs: map[string]string{"o0": "n1"},
					},
					"t2": {
						Inputs:  map[string]string{"i0": "n1"},
						Outputs: map[string]string{},
						Spec: sideInputSpec(map[string]*pipepb.SideInput{
							"i0": {},
						}),
					},
				},
			},
			want: &fusionFacts{
				PcolProducers: map[string]link{
					"n1": {Transform: "t1", Local: "o0", Global: "n1"},
				},
				PcolConsumers: map[string][]link{
					"n1": {{Transform: "t2", Local: "i0", Global: "n1"}},
				},
				UsedAsSideInput: map[string]bool{
					"n1": true,
				},
				DirectSideInputs: map[string]map[string]bool{
					"t1": nil,
					"t2": {"n1": true},
				},
				DownstreamSideInputs: map[string]map[string]bool{
					"t1": {"n1": true},
					"t2": nil,
				},
			},
		}, {
			name:        "t2_consumes_n2_as_side_n1_as_primary_produces_n2",
			topological: []string{"t1", "t2", "t3"},
			comps: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"t1": {
						Inputs:  map[string]string{},
						Outputs: map[string]string{"o0": "n1"},
					},
					"t2": {
						Inputs:  map[string]string{"i0": "n1"},
						Outputs: map[string]string{"o0": "n2"},
						Spec: sideInputSpec(map[string]*pipepb.SideInput{
							"i0": {},
						}),
					},
					"t3": {
						Inputs:  map[string]string{"i0": "n2"},
						Outputs: map[string]string{},
					},
				},
			},
			want: &fusionFacts{
				PcolProducers: map[string]link{
					"n1": {Transform: "t1", Local: "o0", Global: "n1"},
					"n2": {Transform: "t2", Local: "o0", Global: "n2"},
				},
				PcolConsumers: map[string][]link{
					"n1": {{Transform: "t2", Local: "i0", Global: "n1"}},
					"n2": {{Transform: "t3", Local: "i0", Global: "n2"}},
				},
				UsedAsSideInput: map[string]bool{
					"n1": true,
				},
				DirectSideInputs: map[string]map[string]bool{
					"t1": nil,
					"t2": {"n1": true},
					"t3": nil,
				},
				DownstreamSideInputs: map[string]map[string]bool{
					"t1": {"n1": true},
					"t2": nil,
					"t3": nil,
				},
			},
		}, {
			name:        "t3_consumes_n1_as_side_n2_as_primary",
			topological: []string{"t1", "t2", "t3"},
			comps: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"t1": {
						Inputs:  map[string]string{},
						Outputs: map[string]string{"o0": "n1"},
					},
					"t2": {
						Inputs:  map[string]string{},
						Outputs: map[string]string{"o0": "n2"},
					},
					"t3": {
						Inputs:  map[string]string{"i0": "n2", "i1": "n1"},
						Outputs: map[string]string{},
						Spec: sideInputSpec(map[string]*pipepb.SideInput{
							"i1": {},
						}),
					},
				},
			},
			want: &fusionFacts{
				PcolProducers: map[string]link{
					"n1": {Transform: "t1", Local: "o0", Global: "n1"},
					"n2": {Transform: "t2", Local: "o0", Global: "n2"},
				},
				PcolConsumers: map[string][]link{
					"n1": {{Transform: "t3", Local: "i1", Global: "n1"}},
					"n2": {{Transform: "t3", Local: "i0", Global: "n2"}},
				},
				UsedAsSideInput: map[string]bool{
					"n1": true,
				},
				DirectSideInputs: map[string]map[string]bool{
					"t1": nil,
					"t2": nil,
					"t3": {"n1": true},
				},
				DownstreamSideInputs: map[string]map[string]bool{
					"t1": {"n1": true},
					"t2": nil,
					"t3": nil,
				},
			},
		}, {
			name:        "t3_consumes_n2_as_side",
			topological: []string{"t1", "t2", "t3"},
			comps: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"t1": {
						Inputs:  map[string]string{},
						Outputs: map[string]string{"o0": "n1"},
					},
					"t2": {
						Inputs:  map[string]string{"i0": "n1"},
						Outputs: map[string]string{"o0": "n2"},
					},
					"t3": {
						Inputs:  map[string]string{"i1": "n2"},
						Outputs: map[string]string{},
						Spec: sideInputSpec(map[string]*pipepb.SideInput{
							"i1": {},
						}),
					},
				},
			},
			want: &fusionFacts{
				PcolProducers: map[string]link{
					"n1": {Transform: "t1", Local: "o0", Global: "n1"},
					"n2": {Transform: "t2", Local: "o0", Global: "n2"},
				},
				PcolConsumers: map[string][]link{
					"n1": {{Transform: "t2", Local: "i0", Global: "n1"}},
					"n2": {{Transform: "t3", Local: "i1", Global: "n2"}},
				},
				UsedAsSideInput: map[string]bool{
					"n2": true,
				},
				DirectSideInputs: map[string]map[string]bool{
					"t1": nil,
					"t2": nil,
					"t3": {"n2": true},
				},
				DownstreamSideInputs: map[string]map[string]bool{
					"t1": {"n2": true},
					"t2": {"n2": true},
					"t3": nil,
				},
			},
		}, {
			name:        "criss_cross",
			topological: []string{"t1", "t2", "t3", "t4"},
			comps: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"t1": {
						Inputs:  map[string]string{},
						Outputs: map[string]string{"o0": "n1"},
					},
					"t2": {
						Inputs:  map[string]string{},
						Outputs: map[string]string{"o0": "n2"},
					},
					"t3": {
						Inputs:  map[string]string{"i0": "n1", "i1": "n2"},
						Outputs: map[string]string{},
						Spec: sideInputSpec(map[string]*pipepb.SideInput{
							"i1": {},
						}),
					},
					"t4": {
						Inputs:  map[string]string{"i0": "n2", "i1": "n1"},
						Outputs: map[string]string{},
						Spec: sideInputSpec(map[string]*pipepb.SideInput{
							"i1": {},
						}),
					},
				},
			},
			want: &fusionFacts{
				PcolProducers: map[string]link{
					"n1": {Transform: "t1", Local: "o0", Global: "n1"},
					"n2": {Transform: "t2", Local: "o0", Global: "n2"},
				},
				PcolConsumers: map[string][]link{
					"n1": {{Transform: "t3", Local: "i0", Global: "n1"}, {Transform: "t4", Local: "i1", Global: "n1"}},
					"n2": {{Transform: "t3", Local: "i1", Global: "n2"}, {Transform: "t4", Local: "i0", Global: "n2"}},
				},
				UsedAsSideInput: map[string]bool{
					"n1": true,
					"n2": true,
				},
				DirectSideInputs: map[string]map[string]bool{
					"t1": nil,
					"t2": nil,
					"t3": {"n2": true},
					"t4": {"n1": true},
				},
				DownstreamSideInputs: map[string]map[string]bool{
					"t1": {"n1": true},
					"t2": {"n2": true},
					"t3": nil,
					"t4": nil,
				},
			},
		}, {
			name:        "long_criss_cross_tail_cross",
			topological: []string{"r1", "r2", "r3", "l1", "l2", "l3", "r4", "l4"},
			comps: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"r1": {
						Inputs:  map[string]string{},
						Outputs: map[string]string{"o0": "n1"},
					},
					"r2": {
						Inputs:  map[string]string{"i0": "n1"},
						Outputs: map[string]string{"o0": "n2"},
					},
					"r3": {
						Inputs:  map[string]string{"i0": "n2"},
						Outputs: map[string]string{"o0": "n3"},
					},
					"l1": {
						Inputs:  map[string]string{},
						Outputs: map[string]string{"o0": "n4"},
					},
					"l2": {
						Inputs:  map[string]string{"i0": "n4"},
						Outputs: map[string]string{"o0": "n5"},
					},
					"l3": {
						Inputs:  map[string]string{"i0": "n5"},
						Outputs: map[string]string{"o0": "n6"},
					},
					"r4": {
						Inputs:  map[string]string{"i0": "n3", "i1": "n6"},
						Outputs: map[string]string{},
						Spec: sideInputSpec(map[string]*pipepb.SideInput{
							"i1": {},
						}),
					},
					"l4": {
						Inputs:  map[string]string{"i0": "n6", "i1": "n3"},
						Outputs: map[string]string{},
						Spec: sideInputSpec(map[string]*pipepb.SideInput{
							"i1": {},
						}),
					},
				},
			},
			want: &fusionFacts{
				PcolProducers: map[string]link{
					"n1": {Transform: "r1", Local: "o0", Global: "n1"},
					"n2": {Transform: "r2", Local: "o0", Global: "n2"},
					"n3": {Transform: "r3", Local: "o0", Global: "n3"},
					"n4": {Transform: "l1", Local: "o0", Global: "n4"},
					"n5": {Transform: "l2", Local: "o0", Global: "n5"},
					"n6": {Transform: "l3", Local: "o0", Global: "n6"},
				},
				PcolConsumers: map[string][]link{
					"n1": {{Transform: "r2", Local: "i0", Global: "n1"}},
					"n2": {{Transform: "r3", Local: "i0", Global: "n2"}},
					"n3": {{Transform: "r4", Local: "i0", Global: "n3"}, {Transform: "l4", Local: "i1", Global: "n3"}},
					"n4": {{Transform: "l2", Local: "i0", Global: "n4"}},
					"n5": {{Transform: "l3", Local: "i0", Global: "n5"}},
					"n6": {{Transform: "l4", Local: "i0", Global: "n6"}, {Transform: "r4", Local: "i1", Global: "n6"}},
				},
				UsedAsSideInput: map[string]bool{
					"n3": true,
					"n6": true,
				},
				DirectSideInputs: map[string]map[string]bool{
					"r1": nil,
					"r2": nil,
					"r3": nil,
					"r4": {"n6": true},
					"l1": nil,
					"l2": nil,
					"l3": nil,
					"l4": {"n3": true},
				},
				DownstreamSideInputs: map[string]map[string]bool{
					"r1": {"n3": true},
					"r2": {"n3": true},
					"r3": {"n3": true},
					"r4": nil,
					"l1": {"n6": true},
					"l2": {"n6": true},
					"l3": {"n6": true},
					"l4": nil,
				},
			},
		}, {
			name:        "long_criss_cross_head_cross",
			topological: []string{"r1", "r2", "r3", "l1", "l2", "l3", "r4", "l4"},
			comps: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					"r1": {
						Inputs:  map[string]string{},
						Outputs: map[string]string{"o0": "n1"},
					},
					"r2": {
						Inputs:  map[string]string{"i0": "n1"},
						Outputs: map[string]string{"o0": "n2"},
					},
					"r3": {
						Inputs:  map[string]string{"i0": "n2"},
						Outputs: map[string]string{"o0": "n3"},
					},
					"l1": {
						Inputs:  map[string]string{},
						Outputs: map[string]string{"o0": "n4"},
					},
					"l2": {
						Inputs:  map[string]string{"i0": "n4"},
						Outputs: map[string]string{"o0": "n5"},
					},
					"l3": {
						Inputs:  map[string]string{"i0": "n5"},
						Outputs: map[string]string{"o0": "n6"},
					},
					"r4": {
						Inputs:  map[string]string{"i0": "n3", "i1": "n4"},
						Outputs: map[string]string{},
						Spec: sideInputSpec(map[string]*pipepb.SideInput{
							"i1": {},
						}),
					},
					"l4": {
						Inputs:  map[string]string{"i0": "n6", "i1": "n1"},
						Outputs: map[string]string{},
						Spec: sideInputSpec(map[string]*pipepb.SideInput{
							"i1": {},
						}),
					},
				},
			},
			want: &fusionFacts{
				PcolProducers: map[string]link{
					"n1": {Transform: "r1", Local: "o0", Global: "n1"},
					"n2": {Transform: "r2", Local: "o0", Global: "n2"},
					"n3": {Transform: "r3", Local: "o0", Global: "n3"},
					"n4": {Transform: "l1", Local: "o0", Global: "n4"},
					"n5": {Transform: "l2", Local: "o0", Global: "n5"},
					"n6": {Transform: "l3", Local: "o0", Global: "n6"},
				},
				PcolConsumers: map[string][]link{
					"n1": {{Transform: "r2", Local: "i0", Global: "n1"}, {Transform: "l4", Local: "i1", Global: "n1"}},
					"n2": {{Transform: "r3", Local: "i0", Global: "n2"}},
					"n3": {{Transform: "r4", Local: "i0", Global: "n3"}},
					"n4": {{Transform: "l2", Local: "i0", Global: "n4"}, {Transform: "r4", Local: "i1", Global: "n4"}},
					"n5": {{Transform: "l3", Local: "i0", Global: "n5"}},
					"n6": {{Transform: "l4", Local: "i0", Global: "n6"}},
				},
				UsedAsSideInput: map[string]bool{
					"n1": true,
					"n4": true,
				},
				DirectSideInputs: map[string]map[string]bool{
					"r1": nil,
					"r2": nil,
					"r3": nil,
					"r4": {"n4": true},
					"l1": nil,
					"l2": nil,
					"l3": nil,
					"l4": {"n1": true},
				},
				DownstreamSideInputs: map[string]map[string]bool{
					"r1": {"n1": true},
					"r2": nil,
					"r3": nil,
					"r4": nil,
					"l1": {"n4": true},
					"l2": nil,
					"l3": nil,
					"l4": nil,
				},
			},
		},
	}

	linkLess := func(a link, b link) bool {
		if a.Transform != b.Transform {
			return a.Transform < b.Transform
		}
		if a.Local != b.Local {
			return a.Local < b.Local
		}
		return a.Global < b.Global
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := computeFacts(test.topological, test.comps)
			if err != nil {
				t.Fatalf("computeFacts: error: %v", err)
			}
			if d := cmp.Diff(test.want, got, cmp.AllowUnexported(), cmpopts.EquateEmpty(), cmpopts.SortSlices(linkLess)); d != "" {
				t.Errorf("computeFacts diff (-want, +got):\n%v", d)
			}
		})
	}
}
