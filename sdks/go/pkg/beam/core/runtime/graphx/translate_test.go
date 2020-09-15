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

package graphx_test

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
)

func init() {
	runtime.RegisterFunction(pickFn)
	runtime.RegisterType(reflect.TypeOf((*splitPickFn)(nil)).Elem())
}

func pickFn(a int, small, big func(int)) {
	if a < 3 {
		small(a)
	} else {
		big(a)
	}
}

func pickSideFn(a, side int, small, big func(int)) {
	if a < side {
		small(a)
	} else {
		big(a)
	}
}

func addDoFn(t *testing.T, g *graph.Graph, fn interface{}, scope *graph.Scope, inputs []*graph.Node, outputCoders []*coder.Coder, rc *coder.Coder) {
	t.Helper()
	dofn, err := graph.NewDoFn(fn)
	if err != nil {
		t.Fatal(err)
	}
	e, err := graph.NewParDo(g, scope, dofn, inputs, rc, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(outputCoders) != len(e.Output) {
		t.Fatalf("%v has %d outputs, but only got %d coders", dofn.Name(), len(e.Output), len(outputCoders))
	}
	for i, c := range outputCoders {
		e.Output[i].To.Coder = c
	}
}

func newIntInput(g *graph.Graph) *graph.Node {
	in := g.NewNode(intT(), window.DefaultWindowingStrategy(), true)
	in.Coder = intCoder()
	return in
}

func intT() typex.FullType {
	return typex.New(reflectx.Int)
}

func intCoder() *coder.Coder {
	return custom("int", reflectx.Int)
}

// TestMarshal verifies that ParDo can be serialized.
func TestMarshal(t *testing.T) {
	tests := []struct {
		name                     string
		makeGraph                func(t *testing.T, g *graph.Graph)
		edges, transforms, roots int
		requirements             []string
	}{
		{
			name: "ParDo",
			makeGraph: func(t *testing.T, g *graph.Graph) {
				addDoFn(t, g, pickFn, g.Root(), []*graph.Node{newIntInput(g)}, []*coder.Coder{intCoder(), intCoder()}, nil)
			},
			edges:      1,
			transforms: 1,
			roots:      1,
		}, {
			name: "ScopedParDo",
			makeGraph: func(t *testing.T, g *graph.Graph) {
				addDoFn(t, g, pickFn, g.NewScope(g.Root(), "sub"), []*graph.Node{newIntInput(g)}, []*coder.Coder{intCoder(), intCoder()}, nil)
			},
			edges:      1,
			transforms: 2,
			roots:      1,
		}, {
			name: "SplittableParDo",
			makeGraph: func(t *testing.T, g *graph.Graph) {
				addDoFn(t, g, &splitPickFn{}, g.Root(), []*graph.Node{newIntInput(g)}, []*coder.Coder{intCoder(), intCoder()}, intCoder())
			},
			edges:        1,
			transforms:   1,
			roots:        1,
			requirements: []string{graphx.URNRequiresSplittableDoFn},
		}, {
			name: "SideInput",
			makeGraph: func(t *testing.T, g *graph.Graph) {
				in := newIntInput(g)
				side := newIntInput(g)
				addDoFn(t, g, pickSideFn, g.Root(), []*graph.Node{in, side}, []*coder.Coder{intCoder(), intCoder()}, nil)
			},
			edges:      1,
			transforms: 2,
			roots:      2,
		}, {
			name: "ScopedSideInput",
			makeGraph: func(t *testing.T, g *graph.Graph) {
				in := newIntInput(g)
				side := newIntInput(g)
				addDoFn(t, g, pickSideFn, g.NewScope(g.Root(), "sub"), []*graph.Node{in, side}, []*coder.Coder{intCoder(), intCoder()}, nil)
			},
			edges:      1,
			transforms: 3,
			roots:      1,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {

			g := graph.New()
			test.makeGraph(t, g)

			edges, _, err := g.Build()
			if err != nil {
				t.Fatal(err)
			}
			if len(edges) != test.edges {
				t.Fatal("expected a single edge")
			}

			payload, err := proto.Marshal(&pipepb.DockerPayload{ContainerImage: "foo"})
			if err != nil {
				t.Fatal(err)
			}
			p, err := graphx.Marshal(edges,
				&graphx.Options{Environment: &pipepb.Environment{Urn: "beam:env:docker:v1", Payload: payload}})
			if err != nil {
				t.Fatal(err)
			}

			if got, want := len(p.GetComponents().GetTransforms()), test.transforms; got != want {
				t.Errorf("got %d transforms, want %d : %v", got, want, proto.MarshalTextString(p))
			}
			if got, want := len(p.GetRootTransformIds()), test.roots; got != want {
				t.Errorf("got %d roots, want %d : %v", got, want, proto.MarshalTextString(p))
			}
			if got, want := p.GetRequirements(), test.requirements; !cmp.Equal(got, want, cmpopts.SortSlices(func(a, b string) bool { return a < b })) {
				t.Errorf("incorrect requirements: got %v, want %v : %v", got, want, proto.MarshalTextString(p))
			}
		})
	}
}

// testRT's methods can all be no-ops, we just need it to implement sdf.RTracker.
type testRT struct {
}

func (rt *testRT) TryClaim(_ interface{}) bool     { return false }
func (rt *testRT) GetError() error                 { return nil }
func (rt *testRT) GetProgress() (float64, float64) { return 0, 0 }
func (rt *testRT) IsDone() bool                    { return true }
func (rt *testRT) GetRestriction() interface{}     { return nil }
func (rt *testRT) TrySplit(_ float64) (interface{}, interface{}, error) {
	return nil, nil, nil
}

// splitPickFn is used for the SDF test, and just needs to fulfill SDF method
// signatures.
type splitPickFn struct {
}

func (fn *splitPickFn) CreateInitialRestriction(_ int) int   { return 0 }
func (fn *splitPickFn) SplitRestriction(_ int, _ int) []int  { return []int{0} }
func (fn *splitPickFn) RestrictionSize(_ int, _ int) float64 { return 0.0 }
func (fn *splitPickFn) CreateTracker(_ int) *testRT          { return &testRT{} }

// ProcessElement calls pickFn.
func (fn *splitPickFn) ProcessElement(_ *testRT, a int, small, big func(int)) {
	pickFn(a, small, big)
}
