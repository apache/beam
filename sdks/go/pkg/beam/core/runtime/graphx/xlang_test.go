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

package graphx

import (
	"fmt"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func newNode(g *graph.Graph) *graph.Node {
	n := g.NewNode(typex.New(reflectx.Int), window.DefaultWindowingStrategy(), true)
	return n
}

func newIn(g *graph.Graph) *graph.Inbound {
	return &graph.Inbound{
		From: newNode(g),
	}
}

func newIns(g *graph.Graph, n int) []*graph.Inbound {
	var ins []*graph.Inbound
	for i := 0; i < n; i++ {
		ins = append(ins, newIn(g))
	}
	return ins
}

func newOut(g *graph.Graph) *graph.Outbound {
	return &graph.Outbound{
		To: newNode(g),
	}
}

func newOuts(g *graph.Graph, n int) []*graph.Outbound {
	var outs []*graph.Outbound
	for i := 0; i < n; i++ {
		outs = append(outs, newOut(g))
	}
	return outs
}

func newEdge(g *graph.Graph, ins, outs int) *graph.MultiEdge {
	return &graph.MultiEdge{
		Input:  newIns(g, ins),
		Output: newOuts(g, outs),
	}
}

func newExternal(ins, outs map[string]int) *graph.ExternalTransform {
	return &graph.ExternalTransform{
		InputsMap:  ins,
		OutputsMap: outs,
	}
}

type testExternalConf struct {
	i    int
	o    int
	iMap map[string]int
	oMap map[string]int
}

func TestExternalInputs(t *testing.T) {
	tt := testExternalConf{i: 2, o: 3, iMap: map[string]int{"x": 1}, oMap: map[string]int{"y": 1}}

	g := graph.New()
	e := newEdge(g, tt.i, tt.o)
	e.External = newExternal(tt.iMap, tt.oMap)

	i := ExternalInputs(e)

	for tag, idx := range tt.iMap {
		got, exists := i[tag]
		want := e.Input[idx].From

		if !exists {
			t.Errorf("input absent for key %v; expected %v", tag, want)
		}

		if got.ID() != want.ID() {
			t.Errorf("wrong input associated with key %v; want %v but got %v", tag, want, got)
		}
	}
}

func TestExternalOutputs(t *testing.T) {
	tt := testExternalConf{i: 2, o: 3, iMap: map[string]int{"x": 1}, oMap: map[string]int{"y": 1}}

	g := graph.New()
	e := newEdge(g, tt.i, tt.o)
	e.External = newExternal(tt.iMap, tt.oMap)

	o := ExternalOutputs(e)

	for tag, idx := range tt.oMap {
		got, exists := o[tag]
		want := e.Output[idx].To

		if !exists {
			t.Errorf("output absent for key %v; expected %v", tag, want)
		}

		if got.ID() != want.ID() {
			t.Errorf("wrong output associated with key %v; want %v but got %v", tag, want, got)
		}
	}
}

func newTransform(name string) *pipepb.PTransform {
	return &pipepb.PTransform{
		UniqueName: name,
	}
}

func newComponents(ts []string) *pipepb.Components {
	components := &pipepb.Components{}

	components.Transforms = make(map[string]*pipepb.PTransform)
	for id, t := range ts {
		components.Transforms[fmt.Sprint(id)] = newTransform(t)
	}

	return components
}

func TestExpandedTransform(t *testing.T) {
	t.Run("Correct PTransform", func(t *testing.T) {
		want := newTransform("x")
		exp := &graph.ExpandedTransform{Transform: want}

		got, err := ExpandedTransform(exp)
		if err != nil {
			t.Fatal(err)
		}

		if d := cmp.Diff(want, got, protocmp.Transform()); d != "" {
			t.Errorf("diff (-want, +got): %v", d)
		}

	})

	t.Run("Malformed PTransform", func(t *testing.T) {
		exp := &graph.ExpandedTransform{Transform: "gibberish"}
		expectedError := fmt.Sprintf("malformed transform; %v lacks a conforming pipeline ptransform", exp)
		if _, actualError := ExpandedTransform(exp); actualError.Error() != expectedError {
			t.Errorf("got error %v, want error %v", actualError, expectedError)
		}

	})
}

func TestExpandedComponents(t *testing.T) {
	t.Run("Correct Components", func(t *testing.T) {
		want := newComponents([]string{"x"})
		exp := &graph.ExpandedTransform{Components: want}

		got, err := ExpandedComponents(exp)

		if err != nil {
			t.Fatal(err)
		}

		if d := cmp.Diff(want, got, protocmp.Transform()); d != "" {
			t.Errorf("diff (-want, +got): %v", d)
		}

	})

	t.Run("Malformed Components", func(t *testing.T) {
		exp := &graph.ExpandedTransform{Transform: "gibberish"}
		expectedError := fmt.Sprintf("malformed components; %v lacks a conforming pipeline component", exp)
		if _, actualError := ExpandedComponents(exp); actualError.Error() != expectedError {
			t.Errorf("got error %v, want error %v", actualError, expectedError)
		}

	})
}

var testExternal = graph.ExternalTransform{
	Urn:           "test_urn",
	Payload:       nil,
	ExpansionAddr: "test_addr",
	Expanded:      &testExpanded,
}

var testComponents = pipepb.Components{
	Transforms:          map[string]*pipepb.PTransform{"transform_id1": {UniqueName: "test_components_transform"}},
	Pcollections:        map[string]*pipepb.PCollection{"pcollection_id1": {UniqueName: "test_components_pcollection"}},
	WindowingStrategies: map[string]*pipepb.WindowingStrategy{"windowing_id1": {WindowCoderId: "test_components_windowing"}},
	Coders:              map[string]*pipepb.Coder{"coder_id1": {Spec: &pipepb.FunctionSpec{Urn: "test_components_coder"}}},
	Environments:        map[string]*pipepb.Environment{"environment_id1": {Urn: "test_components_environment"}},
}

var testRequirements = []string{"test_requirement1", "test_requirement2"}

var testTransform = pipepb.PTransform{
	UniqueName: "test_transform",
}

var testExpanded = graph.ExpandedTransform{
	Components:   &testComponents,
	Transform:    &testTransform,
	Requirements: testRequirements,
}

var testExternal2 = graph.ExternalTransform{
	Urn:           "test_urn2",
	Payload:       nil,
	ExpansionAddr: "test_addr2",
	Expanded:      &testExpanded2,
}

var testComponents2 = pipepb.Components{
	Transforms:          map[string]*pipepb.PTransform{"transform_id2": {UniqueName: "test_components2_transform"}},
	Pcollections:        map[string]*pipepb.PCollection{"pcollection_id2": {UniqueName: "test_components2_pcollection"}},
	WindowingStrategies: map[string]*pipepb.WindowingStrategy{"windowing_id2": {WindowCoderId: "test_components2_windowing"}},
	Coders:              map[string]*pipepb.Coder{"coder_id2": {Spec: &pipepb.FunctionSpec{Urn: "test_components2_coder"}}},
	Environments:        map[string]*pipepb.Environment{"environment_id2": {Urn: "test_components2_environment"}},
}

var testRequirements2 = []string{"test_requirement2", "test_requirement3"}

var testTransform2 = pipepb.PTransform{
	UniqueName: "test_transform2",
}

var testExpanded2 = graph.ExpandedTransform{
	Components:   &testComponents2,
	Transform:    &testTransform2,
	Requirements: testRequirements2,
}

// TestMergeExpandedWithPipeline tests that mergeExpandedWithPipeline properly
// adds data from external transforms to a pipeline.
func TestMergeExpandedWithPipeline(t *testing.T) {
	// Create slices of MultiEdges containing external edges, and make sure all
	// relevant data from the external edges is properly added to an empty
	// pipeline.
	tests := []struct {
		name          string
		makeEdges     func(g *graph.Graph) []*graph.MultiEdge
		wantExpandeds []graph.ExpandedTransform
	}{
		{
			name: "SingleTransform",
			makeEdges: func(g *graph.Graph) []*graph.MultiEdge {
				s := g.Root()
				edge := g.NewEdge(s)
				edge.Op = graph.External
				edge.External = &testExternal

				return []*graph.MultiEdge{edge}
			},
			wantExpandeds: []graph.ExpandedTransform{testExpanded},
		},
		{
			name: "MultiTransforms",
			makeEdges: func(g *graph.Graph) []*graph.MultiEdge {
				s := g.Root()
				edge := g.NewEdge(s)
				edge.Op = graph.External
				edge.External = &testExternal
				edge2 := g.NewEdge(s)
				edge2.Op = graph.External
				edge2.External = &testExternal2

				return []*graph.MultiEdge{edge, edge2}
			},
			wantExpandeds: []graph.ExpandedTransform{testExpanded, testExpanded2},
		},
		{
			name: "NonExternalTransforms",
			makeEdges: func(g *graph.Graph) []*graph.MultiEdge {
				s := g.Root()
				edge := g.NewEdge(s)
				edge.Op = graph.External
				edge.External = &testExternal
				edge2 := g.NewEdge(s)
				edge2.Op = graph.External
				edge2.External = &testExternal2
				impulse := graph.NewImpulse(g, s, []byte{1})
				impulse2 := graph.NewImpulse(g, s, []byte{2})

				return []*graph.MultiEdge{edge, edge2, impulse, impulse2}
			},
			wantExpandeds: []graph.ExpandedTransform{testExpanded, testExpanded2},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			g := graph.New()
			edges := test.makeEdges(g)
			var p pipepb.Pipeline
			p.Components = &pipepb.Components{
				Transforms:          make(map[string]*pipepb.PTransform),
				Pcollections:        make(map[string]*pipepb.PCollection),
				WindowingStrategies: make(map[string]*pipepb.WindowingStrategy),
				Coders:              make(map[string]*pipepb.Coder),
				Environments:        make(map[string]*pipepb.Environment),
			}
			mergeExpandedWithPipeline(edges, &p)

			// Check that all wanted expanded components have been added to
			// pipeline components.
			for _, exp := range test.wantExpandeds {
				wantComps := exp.Components.(*pipepb.Components)
				gotComps := p.GetComponents()
				validateComponents(t, wantComps, gotComps)

				// Check that expanded transform is present. Need to search.
				wantTransform := exp.Transform.(*pipepb.PTransform)
				var found bool
				for _, gotTransform := range gotComps.GetTransforms() {
					if cmp.Equal(wantTransform, gotTransform, protocmp.Transform()) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Pipeline components missing expected expanded transform: %v", wantTransform)
				}

				// Check that requirements are present.
				for _, wantReq := range exp.Requirements {
					var found bool
					for _, gotReq := range p.GetRequirements() {
						if wantReq == gotReq {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("Pipeline missing expected requirement: %v", wantReq)
					}
				}
			}
		})
	}
}

// validateComponents validates that the wanted components (wantComps) are
// present in the received components (gotComps), or in other words, that
// wantComps is a subset of gotComps.
func validateComponents(t *testing.T, wantComps, gotComps *pipepb.Components) {
	// Because we're checking that wantComps is a subset of gotComps, we can't
	// just diff both Components directly, and instead need to explicitly search
	// through the various fields.

	// Transforms
	for k, wantVal := range wantComps.GetTransforms() {
		gotVal, ok := gotComps.Transforms[k]
		if !ok {
			t.Errorf("Pipeline components missing expected transform with key \"%v\": %v", k, wantVal)
		} else if d := cmp.Diff(wantVal, gotVal, protocmp.Transform()); d != "" {
			t.Errorf("Pipeline components has unexpected transform with key \"%v\": diff (-want, +got):\n%v", k, d)
		}
	}
	// PCollections
	for k, wantVal := range wantComps.GetPcollections() {
		gotVal, ok := gotComps.Pcollections[k]
		if !ok {
			t.Errorf("Pipeline components missing expected PCollection with key \"%v\": %v", k, wantVal)
		} else if d := cmp.Diff(wantVal, gotVal, protocmp.Transform()); d != "" {
			t.Errorf("Pipeline components has unexpected PCollection with key \"%v\": diff (-want, +got):\n%v", k, d)
		}
	}
	// Windowing Strategies
	for k, wantVal := range wantComps.GetWindowingStrategies() {
		gotVal, ok := gotComps.WindowingStrategies[k]
		if !ok {
			t.Errorf("Pipeline components missing expected windowing strategy with key \"%v\": %v", k, wantVal)
		} else if d := cmp.Diff(wantVal, gotVal, protocmp.Transform()); d != "" {
			t.Errorf("Pipeline components has unexpected windowing strategy with key \"%v\": diff (-want, +got):\n%v", k, d)
		}
	}
	// Coders
	for k, wantVal := range wantComps.GetCoders() {
		gotVal, ok := gotComps.Coders[k]
		if !ok {
			t.Errorf("Pipeline components missing expected coder with key \"%v\": %v", k, wantVal)
		} else if d := cmp.Diff(wantVal, gotVal, protocmp.Transform()); d != "" {
			t.Errorf("Pipeline components has unexpected coder with key \"%v\": diff (-want, +got):\n%v", k, d)
		}
	}
	// Environments
	for k, wantVal := range wantComps.GetEnvironments() {
		gotVal, ok := gotComps.Environments[k]
		if !ok {
			t.Errorf("Pipeline components missing expected environment with key \"%v\": %v", k, wantVal)
		} else if d := cmp.Diff(wantVal, gotVal, protocmp.Transform()); d != "" {
			t.Errorf("Pipeline components has unexpected environment with key \"%v\": diff (-want, +got):\n%v", k, d)
		}
	}
}
