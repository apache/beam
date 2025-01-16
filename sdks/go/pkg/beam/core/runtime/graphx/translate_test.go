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
	"context"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/contextreg"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
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

func addDoFn(t *testing.T, g *graph.Graph, fn any, scope *graph.Scope, inputs []*graph.Node, outputCoders []*coder.Coder, rc *coder.Coder) {
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
			transforms: 1,
			roots:      1,
		}, {
			name: "ScopedSideInput",
			makeGraph: func(t *testing.T, g *graph.Graph) {
				in := newIntInput(g)
				side := newIntInput(g)
				addDoFn(t, g, pickSideFn, g.NewScope(g.Root(), "sub"), []*graph.Node{in, side}, []*coder.Coder{intCoder(), intCoder()}, nil)
			},
			edges:      1,
			transforms: 2,
			roots:      1,
		}, {
			name: "Reshuffle",
			makeGraph: func(t *testing.T, g *graph.Graph) {
				in := newIntInput(g)
				graph.NewReshuffle(g, g.Root(), in)
			},
			edges:      1,
			transforms: 4,
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
			if got, want := len(edges), test.edges; got != want {
				t.Fatalf("got %v edges, want %v", got, want)
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
				t.Errorf("got %d transforms, want %d : %v", got, want, p.String())
			}
			if got, want := len(p.GetRootTransformIds()), test.roots; got != want {
				t.Errorf("got %d roots, want %d : %v", got, want, p.String())
			}
			if got, want := p.GetRequirements(), test.requirements; !cmp.Equal(got, want, cmpopts.SortSlices(func(a, b string) bool { return a < b })) {
				t.Errorf("incorrect requirements: got %v, want %v : %v", got, want, p.String())
			}
		})
	}
}

func TestMarshal_PTransformAnnotations(t *testing.T) {
	var creg contextreg.Registry

	const annotationKey = "myAnnotation"

	// A misused ptransform extractor that, if a context is attached to a scope will add an annotation to those transforms.
	creg.TransformExtractor(func(ctx context.Context) contextreg.TransformMetadata {
		return contextreg.TransformMetadata{
			Annotations: map[string][]byte{
				annotationKey: {42, 42, 42},
			},
		}
	})

	tests := []struct {
		name      string
		makeGraph func(t *testing.T, g *graph.Graph)

		transforms int
	}{
		{
			name: "AnnotationSetOnComposite",
			makeGraph: func(t *testing.T, g *graph.Graph) {
				in := newIntInput(g)
				side := newIntInput(g)
				s := g.NewScope(g.Root(), "sub")
				s.Context = context.Background() // Allow the default annotation to trigger.
				addDoFn(t, g, pickSideFn, s, []*graph.Node{in, side}, []*coder.Coder{intCoder(), intCoder()}, nil)
			},
			transforms: 2,
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

			payload, err := proto.Marshal(&pipepb.DockerPayload{ContainerImage: "foo"})
			if err != nil {
				t.Fatal(err)
			}
			p, err := graphx.Marshal(edges,
				&graphx.Options{Environment: &pipepb.Environment{Urn: "beam:env:docker:v1", Payload: payload}, ContextReg: &creg})
			if err != nil {
				t.Fatal(err)
			}

			pts := p.GetComponents().GetTransforms()
			if got, want := len(pts), test.transforms; got != want {
				t.Errorf("got %d transforms, want %d : %v", got, want, p.String())
			}
			for _, pt := range pts {
				// Context annotations only apply to composites, and are not duplicated to leaves.
				if len(pt.GetSubtransforms()) == 0 {
					if _, ok := pt.GetAnnotations()[annotationKey]; ok {
						t.Errorf("unexpected annotation %v on leaf transform: %v", annotationKey, pt.GetAnnotations())
					}
					continue
				}
				if _, ok := pt.GetAnnotations()[annotationKey]; !ok {
					t.Errorf("expected %q annotation, but wasn't present: %v", annotationKey, pt.GetAnnotations())
				}
			}
		})
	}
}

// testRT's methods can all be no-ops, we just need it to implement sdf.RTracker.
type testRT struct {
}

func (rt *testRT) TryClaim(_ any) bool             { return false }
func (rt *testRT) GetError() error                 { return nil }
func (rt *testRT) GetProgress() (float64, float64) { return 0, 0 }
func (rt *testRT) IsDone() bool                    { return true }
func (rt *testRT) GetRestriction() any             { return nil }
func (rt *testRT) IsBounded() bool                 { return true }
func (rt *testRT) TrySplit(_ float64) (any, any, error) {
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

func TestCreateEnvironment(t *testing.T) {
	t.Run("process", func(t *testing.T) {
		const wantEnv = "process"
		urn := graphx.URNEnvProcess
		got, err := graphx.CreateEnvironment(context.Background(), urn, func(_ context.Context) string { return wantEnv })
		if err == nil {
			t.Errorf("CreateEnvironment(%v) = %v error, want error since it's unsupported", urn, err)
		}
		want := (*pipepb.Environment)(nil)
		if !proto.Equal(got, want) {
			t.Errorf("CreateEnvironment(%v) = %v, want %v since it's unsupported", urn, got, want)
		}
	})
	tests := []struct {
		name    string
		urn     string
		payload func(name string) []byte
	}{
		{
			name: "external",
			urn:  graphx.URNEnvExternal,
			payload: func(name string) []byte {
				return protox.MustEncode(&pipepb.ExternalPayload{
					Endpoint: &pipepb.ApiServiceDescriptor{
						Url: name,
					},
				})
			},
		}, {
			name: "docker",
			urn:  graphx.URNEnvDocker,
			payload: func(name string) []byte {
				return protox.MustEncode(&pipepb.DockerPayload{
					ContainerImage: name,
				})
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, err := graphx.CreateEnvironment(context.Background(), test.urn, func(_ context.Context) string { return test.name })
			if err != nil {
				t.Errorf("CreateEnvironment(%v) = %v error, want nil", test.urn, err)
			}
			want := &pipepb.Environment{
				Urn:     test.urn,
				Payload: test.payload(test.name),
				Dependencies: []*pipepb.ArtifactInformation{
					{
						TypeUrn:     graphx.URNArtifactFileType,
						TypePayload: protox.MustEncode(&pipepb.ArtifactFilePayload{}),
						RoleUrn:     graphx.URNArtifactGoWorkerRole,
					},
				},
			}
			opts := []cmp.Option{
				protocmp.Transform(),
				// Ignore the capabilities field, since we can't access that method here.
				protocmp.IgnoreFields(&pipepb.Environment{}, protoreflect.Name("capabilities")),
			}
			if d := cmp.Diff(want, got, opts...); d != "" {
				t.Errorf("CreateEnvironment(%v) diff (-want, +got):\n%v", test.urn, d)
			}
		})
	}
}

func TestUpdateDefaultEnvWorkerType(t *testing.T) {
	t.Run("noEnvs", func(t *testing.T) {
		if err := graphx.UpdateDefaultEnvWorkerType("unused", nil, &pipepb.Pipeline{
			Components: &pipepb.Components{},
		}); err == nil {
			t.Error("UpdateDefaultEnvWorkerType(<emptyEnv>) no error, want err")
		}
	})
	t.Run("noGoEnvs", func(t *testing.T) {
		if err := graphx.UpdateDefaultEnvWorkerType("unused", nil, &pipepb.Pipeline{
			Components: &pipepb.Components{
				Environments: map[string]*pipepb.Environment{
					"java":       {Urn: "java"},
					"python":     {Urn: "python"},
					"typescript": {Urn: "typescript"},
				},
			},
		}); err == nil {
			t.Error("UpdateDefaultEnvWorkerType(<noGoEnvs>) no error, want err")
		}
	})
	t.Run("badGoEnv", func(t *testing.T) {
		if err := graphx.UpdateDefaultEnvWorkerType("unused", nil, &pipepb.Pipeline{
			Components: &pipepb.Components{
				Environments: map[string]*pipepb.Environment{
					"java":       {Urn: "java"},
					"python":     {Urn: "python"},
					"typescript": {Urn: "typescript"},
					"go": {
						Urn:     "test",
						Payload: []byte("test"),
						Dependencies: []*pipepb.ArtifactInformation{
							{
								RoleUrn: "unset",
							},
						},
					},
				},
			},
		}); err == nil {
			t.Error("UpdateDefaultEnvWorkerType(<badGoEnv>) no error, want err")
		}
	})
	t.Run("goEnv", func(t *testing.T) {
		wantUrn := graphx.URNArtifactFileType
		wantPyld := protox.MustEncode(&pipepb.ArtifactFilePayload{
			Path: "good",
		})
		p := &pipepb.Pipeline{
			Components: &pipepb.Components{
				Environments: map[string]*pipepb.Environment{
					"java":       {Urn: "java"},
					"python":     {Urn: "python"},
					"typescript": {Urn: "typescript"},
					"go": {
						Urn:     "test",
						Payload: []byte("test"),
						Dependencies: []*pipepb.ArtifactInformation{
							{
								TypeUrn:     "to be removed",
								TypePayload: nil,
								RoleUrn:     graphx.URNArtifactGoWorkerRole,
							},
						},
					},
				},
			},
		}
		if err := graphx.UpdateDefaultEnvWorkerType(wantUrn, wantPyld, p); err != nil {
			t.Errorf("UpdateDefaultEnvWorkerType(<goEnv>) = %v, want nil", err)
		}
		got := p.GetComponents().GetEnvironments()["go"]
		want := &pipepb.Environment{
			Urn:     "test",
			Payload: []byte("test"),
			Dependencies: []*pipepb.ArtifactInformation{
				{
					TypeUrn:     wantUrn,
					TypePayload: wantPyld,
					RoleUrn:     graphx.URNArtifactGoWorkerRole,
				},
			},
		}
		opts := []cmp.Option{
			protocmp.Transform(),
		}
		if d := cmp.Diff(want, got, opts...); d != "" {
			t.Errorf("UpdateDefaultEnvWorkerType(<goEnv>) diff (-want, +got):\n%v", d)
		}
	})

}
