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
			name: "NeedsUniqueLeaves",
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
		{
			name: "StripUniqueLeaves",
			in: map[string]*pipepb.PTransform{
				"1": {UniqueName: "a"},
				"2": {UniqueName: "a'1"},
				"3": {UniqueName: "a'2"},
			},
			exp: map[string]*pipepb.PTransform{
				"1": {UniqueName: "a"},
				"2": {UniqueName: "a'1"},
				"3": {UniqueName: "a'2"},
			},
		},
		{
			name: "NonTopologicalIdOrder",
			in: map[string]*pipepb.PTransform{
				"e1": {UniqueName: "a"},
				"s1": {UniqueName: "a", Subtransforms: []string{"e1"}},
				"s2": {UniqueName: "a", Subtransforms: []string{"s1"}},
				"s3": {UniqueName: "a", Subtransforms: []string{"s2"}}, // root
			},
			exp: map[string]*pipepb.PTransform{
				"e1": {UniqueName: "a/a/a/a"},
				"s1": {UniqueName: "a/a/a", Subtransforms: []string{"e1"}},
				"s2": {UniqueName: "a/a", Subtransforms: []string{"s1"}},
				"s3": {UniqueName: "a", Subtransforms: []string{"s2"}}, // root
			},
		},
		{
			name: "UniqueComps",
			in: map[string]*pipepb.PTransform{
				"e1": {UniqueName: "a"},
				"e2": {UniqueName: "a"},
				"s1": {UniqueName: "a", Subtransforms: []string{"e1"}},
				"s2": {UniqueName: "a", Subtransforms: []string{"e2"}},
			},
			exp: map[string]*pipepb.PTransform{
				"e1": {UniqueName: "a/a"},
				"e2": {UniqueName: "a'1/a"},
				"s1": {UniqueName: "a", Subtransforms: []string{"e1"}},
				"s2": {UniqueName: "a'1", Subtransforms: []string{"e2"}},
			},
		},
		{
			name: "StripComps",
			in: map[string]*pipepb.PTransform{
				"e1": {UniqueName: "a/a"},
				"e2": {UniqueName: "a'1/a"},
				"s1": {UniqueName: "a", Subtransforms: []string{"e1"}},
				"s2": {UniqueName: "a'1", Subtransforms: []string{"e2"}},
			},
			exp: map[string]*pipepb.PTransform{
				"e1": {UniqueName: "a/a"},
				"e2": {UniqueName: "a'1/a"},
				"s1": {UniqueName: "a", Subtransforms: []string{"e1"}},
				"s2": {UniqueName: "a'1", Subtransforms: []string{"e2"}},
			},
		},
		{
			name: "large",
			in: map[string]*pipepb.PTransform{
				"e1":  {UniqueName: "a"},
				"e2":  {UniqueName: "a"},
				"e3":  {UniqueName: "a"},
				"e4":  {UniqueName: "a"},
				"e5":  {UniqueName: "a"},
				"e6":  {UniqueName: "a"},
				"e7":  {UniqueName: "a"},
				"e8":  {UniqueName: "a"},
				"e9":  {UniqueName: "a"},
				"e10": {UniqueName: "a"},
				"e11": {UniqueName: "a"},
				"e12": {UniqueName: "a"},
				"s1":  {UniqueName: "a", Subtransforms: []string{"s2", "s3"}},
				"s2":  {UniqueName: "a", Subtransforms: []string{"s4", "s5"}},
				"s3":  {UniqueName: "a", Subtransforms: []string{"s6", "s7"}},
				"s4":  {UniqueName: "a", Subtransforms: []string{"e1"}},
				"s5":  {UniqueName: "a", Subtransforms: []string{"e2", "e3"}},
				"s6":  {UniqueName: "a", Subtransforms: []string{"e4", "e5", "e6"}},
				"s7":  {UniqueName: "a", Subtransforms: []string{"e7", "e8", "e9", "e10", "e11", "e12"}},
			},
			exp: map[string]*pipepb.PTransform{
				"e1":  {UniqueName: "a/a/a/a"},
				"e2":  {UniqueName: "a/a/a'1/a"},
				"e3":  {UniqueName: "a/a/a'1/a'1"},
				"e4":  {UniqueName: "a/a'1/a/a"},
				"e5":  {UniqueName: "a/a'1/a/a'1"},
				"e6":  {UniqueName: "a/a'1/a/a'2"},
				"e7":  {UniqueName: "a/a'1/a'1/a"},
				"e8":  {UniqueName: "a/a'1/a'1/a'1"},
				"e9":  {UniqueName: "a/a'1/a'1/a'2"},
				"e10": {UniqueName: "a/a'1/a'1/a'3"},
				"e11": {UniqueName: "a/a'1/a'1/a'4"},
				"e12": {UniqueName: "a/a'1/a'1/a'5"},
				"s1":  {UniqueName: "a", Subtransforms: []string{"s2", "s3"}},
				"s2":  {UniqueName: "a/a", Subtransforms: []string{"s4", "s5"}},
				"s3":  {UniqueName: "a/a'1", Subtransforms: []string{"s6", "s7"}},
				"s4":  {UniqueName: "a/a/a", Subtransforms: []string{"e1"}},
				"s5":  {UniqueName: "a/a/a'1", Subtransforms: []string{"e2", "e3"}},
				"s6":  {UniqueName: "a/a'1/a", Subtransforms: []string{"e4", "e5", "e6"}},
				"s7":  {UniqueName: "a/a'1/a'1", Subtransforms: []string{"e7", "e8", "e9", "e10", "e11", "e12"}},
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

func BenchmarkComputeInputOutput(b *testing.B) {
	in := make(map[string]*pipepb.PTransform)
	// Build a long chain of composite transforms.
	for i := 0; i < 3000; i++ {
		compositeID := fmt.Sprintf("x%d", i)
		primitiveID := fmt.Sprintf("y%d", i)
		in[compositeID] = &pipepb.PTransform{
			UniqueName:    compositeID,
			Subtransforms: []string{primitiveID},
		}
		in[primitiveID] = &pipepb.PTransform{
			UniqueName: primitiveID,
			Inputs:     map[string]string{"i0": fmt.Sprintf("p%d", i)},
			Outputs:    map[string]string{"i0": fmt.Sprintf("p%d", i+1)},
		}
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

			p := &pipepb.Pipeline{
				Components: &pipepb.Components{
					Environments: envs,
				},
			}
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
	env := &pipepb.Environment{
		Urn:          "alpha",
		DisplayData:  []*pipepb.DisplayData{{Urn: "beta"}},
		Capabilities: []string{"delta", "gamma"},
	}
	pl := pipepb.DockerPayload{ContainerImage: containerImg}
	plb, err := proto.Marshal(&pl)
	if err != nil {
		t.Fatalf("Failed to marshal DockerPayload with container image %v: %v", containerImg, err)
	}
	env.Payload = plb
	return env
}
