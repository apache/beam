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
	"fmt"
	"testing"

	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestEnsureUniqueName(t *testing.T) {
	tests := []struct {
		name    string
		in, exp map[string]*pipepb.PTransform
	}{
		{
			name: "AlreadyUnique",
			in: map[string]*pipepb.PTransform{
				"1": pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"2": pipepb.PTransform_builder{UniqueName: "b"}.Build(),
				"3": pipepb.PTransform_builder{UniqueName: "c"}.Build(),
			},
			exp: map[string]*pipepb.PTransform{
				"1": pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"2": pipepb.PTransform_builder{UniqueName: "b"}.Build(),
				"3": pipepb.PTransform_builder{UniqueName: "c"}.Build(),
			},
		},
		{
			name: "NeedsUniqueLeaves",
			in: map[string]*pipepb.PTransform{
				"2": pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"1": pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"3": pipepb.PTransform_builder{UniqueName: "a"}.Build(),
			},
			exp: map[string]*pipepb.PTransform{
				"1": pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"2": pipepb.PTransform_builder{UniqueName: "a'1"}.Build(),
				"3": pipepb.PTransform_builder{UniqueName: "a'2"}.Build(),
			},
		},
		{
			name: "StripUniqueLeaves",
			in: map[string]*pipepb.PTransform{
				"1": pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"2": pipepb.PTransform_builder{UniqueName: "a'1"}.Build(),
				"3": pipepb.PTransform_builder{UniqueName: "a'2"}.Build(),
			},
			exp: map[string]*pipepb.PTransform{
				"1": pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"2": pipepb.PTransform_builder{UniqueName: "a'1"}.Build(),
				"3": pipepb.PTransform_builder{UniqueName: "a'2"}.Build(),
			},
		},
		{
			name: "NonTopologicalIdOrder",
			in: map[string]*pipepb.PTransform{
				"e1": pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"s1": pipepb.PTransform_builder{UniqueName: "a", Subtransforms: []string{"e1"}}.Build(),
				"s2": pipepb.PTransform_builder{UniqueName: "a", Subtransforms: []string{"s1"}}.Build(),
				"s3": pipepb.PTransform_builder{UniqueName: "a", Subtransforms: []string{"s2"}}.Build(), // root
			},
			exp: map[string]*pipepb.PTransform{
				"e1": pipepb.PTransform_builder{UniqueName: "a/a/a/a"}.Build(),
				"s1": pipepb.PTransform_builder{UniqueName: "a/a/a", Subtransforms: []string{"e1"}}.Build(),
				"s2": pipepb.PTransform_builder{UniqueName: "a/a", Subtransforms: []string{"s1"}}.Build(),
				"s3": pipepb.PTransform_builder{UniqueName: "a", Subtransforms: []string{"s2"}}.Build(), // root
			},
		},
		{
			name: "UniqueComps",
			in: map[string]*pipepb.PTransform{
				"e1": pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"e2": pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"s1": pipepb.PTransform_builder{UniqueName: "a", Subtransforms: []string{"e1"}}.Build(),
				"s2": pipepb.PTransform_builder{UniqueName: "a", Subtransforms: []string{"e2"}}.Build(),
			},
			exp: map[string]*pipepb.PTransform{
				"e1": pipepb.PTransform_builder{UniqueName: "a/a"}.Build(),
				"e2": pipepb.PTransform_builder{UniqueName: "a'1/a"}.Build(),
				"s1": pipepb.PTransform_builder{UniqueName: "a", Subtransforms: []string{"e1"}}.Build(),
				"s2": pipepb.PTransform_builder{UniqueName: "a'1", Subtransforms: []string{"e2"}}.Build(),
			},
		},
		{
			name: "StripComps",
			in: map[string]*pipepb.PTransform{
				"e1": pipepb.PTransform_builder{UniqueName: "a/a"}.Build(),
				"e2": pipepb.PTransform_builder{UniqueName: "a'1/a"}.Build(),
				"s1": pipepb.PTransform_builder{UniqueName: "a", Subtransforms: []string{"e1"}}.Build(),
				"s2": pipepb.PTransform_builder{UniqueName: "a'1", Subtransforms: []string{"e2"}}.Build(),
			},
			exp: map[string]*pipepb.PTransform{
				"e1": pipepb.PTransform_builder{UniqueName: "a/a"}.Build(),
				"e2": pipepb.PTransform_builder{UniqueName: "a'1/a"}.Build(),
				"s1": pipepb.PTransform_builder{UniqueName: "a", Subtransforms: []string{"e1"}}.Build(),
				"s2": pipepb.PTransform_builder{UniqueName: "a'1", Subtransforms: []string{"e2"}}.Build(),
			},
		},
		{
			name: "large",
			in: map[string]*pipepb.PTransform{
				"e1":  pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"e2":  pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"e3":  pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"e4":  pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"e5":  pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"e6":  pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"e7":  pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"e8":  pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"e9":  pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"e10": pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"e11": pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"e12": pipepb.PTransform_builder{UniqueName: "a"}.Build(),
				"s1":  pipepb.PTransform_builder{UniqueName: "a", Subtransforms: []string{"s2", "s3"}}.Build(),
				"s2":  pipepb.PTransform_builder{UniqueName: "a", Subtransforms: []string{"s4", "s5"}}.Build(),
				"s3":  pipepb.PTransform_builder{UniqueName: "a", Subtransforms: []string{"s6", "s7"}}.Build(),
				"s4":  pipepb.PTransform_builder{UniqueName: "a", Subtransforms: []string{"e1"}}.Build(),
				"s5":  pipepb.PTransform_builder{UniqueName: "a", Subtransforms: []string{"e2", "e3"}}.Build(),
				"s6":  pipepb.PTransform_builder{UniqueName: "a", Subtransforms: []string{"e4", "e5", "e6"}}.Build(),
				"s7":  pipepb.PTransform_builder{UniqueName: "a", Subtransforms: []string{"e7", "e8", "e9", "e10", "e11", "e12"}}.Build(),
			},
			exp: map[string]*pipepb.PTransform{
				"e1":  pipepb.PTransform_builder{UniqueName: "a/a/a/a"}.Build(),
				"e2":  pipepb.PTransform_builder{UniqueName: "a/a/a'1/a"}.Build(),
				"e3":  pipepb.PTransform_builder{UniqueName: "a/a/a'1/a'1"}.Build(),
				"e4":  pipepb.PTransform_builder{UniqueName: "a/a'1/a/a"}.Build(),
				"e5":  pipepb.PTransform_builder{UniqueName: "a/a'1/a/a'1"}.Build(),
				"e6":  pipepb.PTransform_builder{UniqueName: "a/a'1/a/a'2"}.Build(),
				"e7":  pipepb.PTransform_builder{UniqueName: "a/a'1/a'1/a"}.Build(),
				"e8":  pipepb.PTransform_builder{UniqueName: "a/a'1/a'1/a'1"}.Build(),
				"e9":  pipepb.PTransform_builder{UniqueName: "a/a'1/a'1/a'2"}.Build(),
				"e10": pipepb.PTransform_builder{UniqueName: "a/a'1/a'1/a'3"}.Build(),
				"e11": pipepb.PTransform_builder{UniqueName: "a/a'1/a'1/a'4"}.Build(),
				"e12": pipepb.PTransform_builder{UniqueName: "a/a'1/a'1/a'5"}.Build(),
				"s1":  pipepb.PTransform_builder{UniqueName: "a", Subtransforms: []string{"s2", "s3"}}.Build(),
				"s2":  pipepb.PTransform_builder{UniqueName: "a/a", Subtransforms: []string{"s4", "s5"}}.Build(),
				"s3":  pipepb.PTransform_builder{UniqueName: "a/a'1", Subtransforms: []string{"s6", "s7"}}.Build(),
				"s4":  pipepb.PTransform_builder{UniqueName: "a/a/a", Subtransforms: []string{"e1"}}.Build(),
				"s5":  pipepb.PTransform_builder{UniqueName: "a/a/a'1", Subtransforms: []string{"e2", "e3"}}.Build(),
				"s6":  pipepb.PTransform_builder{UniqueName: "a/a'1/a", Subtransforms: []string{"e4", "e5", "e6"}}.Build(),
				"s7":  pipepb.PTransform_builder{UniqueName: "a/a'1/a'1", Subtransforms: []string{"e7", "e8", "e9", "e10", "e11", "e12"}}.Build(),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := ensureUniqueNames(test.in)
			if d := cmp.Diff(test.exp, actual, protocmp.Transform()); d != "" {
				t.Errorf("ensureUniqueName(%v) = %v, want %v\n %v", test.in, actual, test.exp, d)
			}
		})
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
				"1": pipepb.PTransform_builder{
					UniqueName:    "a",
					Subtransforms: []string{"2"},
				}.Build(),
				"2": pipepb.PTransform_builder{
					UniqueName: "b",
					Inputs:     map[string]string{"i0": "p1"},
					Outputs:    map[string]string{"i0": "p2"},
				}.Build(),
			},
			exp: map[string]*pipepb.PTransform{
				"1": pipepb.PTransform_builder{
					UniqueName:    "a",
					Subtransforms: []string{"2"},
					Inputs:        map[string]string{"p1": "p1"},
					Outputs:       map[string]string{"p2": "p2"},
				}.Build(),
				"2": pipepb.PTransform_builder{
					UniqueName: "b",
					Inputs:     map[string]string{"i0": "p1"},
					Outputs:    map[string]string{"i0": "p2"},
				}.Build(),
			},
		},
		{
			name: "closed composite",
			in: map[string]*pipepb.PTransform{
				"1": pipepb.PTransform_builder{
					UniqueName:    "a",
					Subtransforms: []string{"2", "3"},
				}.Build(),
				"2": pipepb.PTransform_builder{UniqueName: "b", Outputs: map[string]string{"i0": "p1"}}.Build(),
				"3": pipepb.PTransform_builder{UniqueName: "c", Inputs: map[string]string{"i0": "p1"}}.Build(),
			},
			exp: map[string]*pipepb.PTransform{
				"1": pipepb.PTransform_builder{
					UniqueName:    "a",
					Subtransforms: []string{"2", "3"},
				}.Build(),
				"2": pipepb.PTransform_builder{UniqueName: "b", Outputs: map[string]string{"i0": "p1"}}.Build(),
				"3": pipepb.PTransform_builder{UniqueName: "c", Inputs: map[string]string{"i0": "p1"}}.Build(),
			},
		},
		{
			name: "nested composites",
			in: map[string]*pipepb.PTransform{
				"1": pipepb.PTransform_builder{
					UniqueName:    "a",
					Subtransforms: []string{"2"},
				}.Build(),
				"2": pipepb.PTransform_builder{
					UniqueName:    "b",
					Subtransforms: []string{"3", "7", "8"},
				}.Build(),
				"3": pipepb.PTransform_builder{
					UniqueName:    "c",
					Subtransforms: []string{"4", "5", "6"},
				}.Build(),
				"4": pipepb.PTransform_builder{UniqueName: "d", Inputs: map[string]string{"i0": "p1"}, Outputs: map[string]string{"i0": "p2"}}.Build(),
				"5": pipepb.PTransform_builder{UniqueName: "e", Inputs: map[string]string{"i0": "p2"}, Outputs: map[string]string{"i0": "p3", "i1": "p4"}}.Build(),
				"6": pipepb.PTransform_builder{UniqueName: "f", Inputs: map[string]string{"i0": "p2", "i1": "p5"}, Outputs: map[string]string{"i0": "p6"}}.Build(),
				"7": pipepb.PTransform_builder{UniqueName: "g", Inputs: map[string]string{"i0": "p4", "i1": "p6", "i2": "p8"}, Outputs: map[string]string{"i0": "p7"}}.Build(),
				"8": pipepb.PTransform_builder{UniqueName: "h", Inputs: map[string]string{"i0": "p7"}}.Build(),
			},
			exp: map[string]*pipepb.PTransform{
				"1": pipepb.PTransform_builder{
					UniqueName:    "a",
					Subtransforms: []string{"2"},
					Inputs:        map[string]string{"p1": "p1", "p5": "p5", "p8": "p8"},
					Outputs:       map[string]string{"p3": "p3"},
				}.Build(),
				"2": pipepb.PTransform_builder{
					UniqueName:    "b",
					Subtransforms: []string{"3", "7", "8"},
					Inputs:        map[string]string{"p1": "p1", "p5": "p5", "p8": "p8"},
					Outputs:       map[string]string{"p3": "p3"},
				}.Build(),
				"3": pipepb.PTransform_builder{
					UniqueName:    "c",
					Subtransforms: []string{"4", "6", "5"}, // topologically sorted.
					Inputs:        map[string]string{"p1": "p1", "p5": "p5"},
					Outputs:       map[string]string{"p4": "p4", "p6": "p6", "p3": "p3"},
				}.Build(),
				"4": pipepb.PTransform_builder{UniqueName: "d", Inputs: map[string]string{"i0": "p1"}, Outputs: map[string]string{"i0": "p2"}}.Build(),
				"5": pipepb.PTransform_builder{UniqueName: "e", Inputs: map[string]string{"i0": "p2"}, Outputs: map[string]string{"i0": "p3", "i1": "p4"}}.Build(),
				"6": pipepb.PTransform_builder{UniqueName: "f", Inputs: map[string]string{"i0": "p2", "i1": "p5"}, Outputs: map[string]string{"i0": "p6"}}.Build(),
				"7": pipepb.PTransform_builder{UniqueName: "g", Inputs: map[string]string{"i0": "p4", "i1": "p6", "i2": "p8"}, Outputs: map[string]string{"i0": "p7"}}.Build(),
				"8": pipepb.PTransform_builder{UniqueName: "h", Inputs: map[string]string{"i0": "p7"}}.Build(),
			},
		}, {
			name: "sibling composite",
			in: map[string]*pipepb.PTransform{
				"1": pipepb.PTransform_builder{
					UniqueName:    "a",
					Subtransforms: []string{"3", "4"},
				}.Build(),
				"2": pipepb.PTransform_builder{
					UniqueName:    "b",
					Subtransforms: []string{"5"},
				}.Build(),
				"3": pipepb.PTransform_builder{UniqueName: "c", Outputs: map[string]string{"i0": "p1"}}.Build(),
				"4": pipepb.PTransform_builder{UniqueName: "d", Inputs: map[string]string{"i0": "p1"}}.Build(),
				"5": pipepb.PTransform_builder{UniqueName: "e", Inputs: map[string]string{"i0": "p1"}}.Build(),
			},
			exp: map[string]*pipepb.PTransform{
				"1": pipepb.PTransform_builder{
					UniqueName:    "a",
					Subtransforms: []string{"3", "4"},
					Outputs:       map[string]string{"p1": "p1"},
				}.Build(),
				"2": pipepb.PTransform_builder{
					UniqueName:    "b",
					Subtransforms: []string{"5"},
					Inputs:        map[string]string{"p1": "p1"},
				}.Build(),
				"3": pipepb.PTransform_builder{UniqueName: "c", Outputs: map[string]string{"i0": "p1"}}.Build(),
				"4": pipepb.PTransform_builder{UniqueName: "d", Inputs: map[string]string{"i0": "p1"}}.Build(),
				"5": pipepb.PTransform_builder{UniqueName: "e", Inputs: map[string]string{"i0": "p1"}}.Build(),
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

func BenchmarkComputeInputOutput(b *testing.B) {
	in := make(map[string]*pipepb.PTransform)
	// Build a long chain of composite transforms.
	for i := 0; i < 3000; i++ {
		compositeID := fmt.Sprintf("x%d", i)
		primitiveID := fmt.Sprintf("y%d", i)
		in[compositeID] = pipepb.PTransform_builder{
			UniqueName:    compositeID,
			Subtransforms: []string{primitiveID},
		}.Build()
		in[primitiveID] = pipepb.PTransform_builder{
			UniqueName: primitiveID,
			Inputs:     map[string]string{"i0": fmt.Sprintf("p%d", i)},
			Outputs:    map[string]string{"i0": fmt.Sprintf("p%d", i+1)},
		}.Build()
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		computeCompositeInputOutput(in)
	}
}

func TestApplySdkImageOverrides(t *testing.T) {
	tests := []struct {
		name     string
		patterns map[string]string
		envs     map[string]string // Environment ID to container image name.
		want     map[string]string // Environment ID to final container image names.
	}{
		{
			name:     "Basic",
			patterns: map[string]string{".*foo.*": "foo:override"},
			envs: map[string]string{
				"foobar": "foo:invalid",
				"bar":    "bar:valid",
			},
			want: map[string]string{
				"foobar": "foo:override",
				"bar":    "bar:valid",
			},
		},
		{
			name: "MultiplePatterns",
			patterns: map[string]string{
				".*foo.*": "foo:override",
				".*bar.*": "bar:override",
			},
			envs: map[string]string{
				"foobaz": "foo:invalid",
				"barbaz": "bar:invalid",
			},
			want: map[string]string{
				"foobaz": "foo:override",
				"barbaz": "bar:override",
			},
		},
		{
			name:     "MultipleMatches",
			patterns: map[string]string{".*foo.*": "foo:override"},
			envs: map[string]string{
				"foo1": "foo1:invalid",
				"foo2": "foo2:invalid",
			},
			want: map[string]string{
				"foo1": "foo:override",
				"foo2": "foo:override",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			envs := make(map[string]*pipepb.Environment)
			for id, ci := range test.envs {
				env := buildEnvironment(t, ci)
				envs[id] = env
			}
			wantEnvs := make(map[string]*pipepb.Environment)
			for id, ci := range test.want {
				env := buildEnvironment(t, ci)
				wantEnvs[id] = env
			}

			p := pipepb.Pipeline_builder{
				Components: pipepb.Components_builder{
					Environments: envs,
				}.Build(),
			}.Build()
			if err := ApplySdkImageOverrides(p, test.patterns); err != nil {
				t.Fatalf("ApplySdkImageOverrides failed: %v", err)
			}
			if diff := cmp.Diff(envs, wantEnvs, protocmp.Transform()); diff != "" {
				t.Errorf("ApplySdkImageOverrides gave incorrect output: diff(-want,+got):\n %v", diff)
			}
		})
	}
}

func buildEnvironment(t *testing.T, containerImg string) *pipepb.Environment {
	t.Helper()
	env := pipepb.Environment_builder{
		Urn:          "alpha",
		DisplayData:  []*pipepb.DisplayData{pipepb.DisplayData_builder{Urn: "beta"}.Build()},
		Capabilities: []string{"delta", "gamma"},
	}.Build()
	pl := pipepb.DockerPayload_builder{ContainerImage: containerImg}.Build()
	plb, err := proto.Marshal(pl)
	if err != nil {
		t.Fatalf("Failed to marshal DockerPayload with container image %v: %v", containerImg, err)
	}
	env.SetPayload(plb)
	return env
}
