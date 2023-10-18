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

	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
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
			name: "preparer",
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
					outputs: []link{{transform: "e1_early", local: "i0", global: "pcol1"}}},
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
			pre := newPreprocessor([]transformPreparer{&testPreparer{}})

			gotStages := pre.preProcessGraph(test.input)
			if diff := cmp.Diff(test.wantStages, gotStages, cmp.AllowUnexported(stage{}, link{}), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("preProcessGraph(%q) stages diff (-want,+got)\n%v", test.name, diff)
			}

			if diff := cmp.Diff(test.wantComponents, test.input, protocmp.Transform()); diff != "" {
				t.Errorf("preProcessGraph(%q) components diff (-want,+got)\n%v", test.name, diff)
			}
		})
	}
}

type testPreparer struct{}

func (p *testPreparer) PrepareUrns() []string {
	return []string{"test_urn"}
}

func (p *testPreparer) PrepareTransform(tid string, t *pipepb.PTransform, comps *pipepb.Components) (*pipepb.Components, []string) {
	return &pipepb.Components{
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
	}, []string{"e1"}
}
