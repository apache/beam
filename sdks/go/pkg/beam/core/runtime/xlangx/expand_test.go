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
	"context"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/resource"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
)

// nonPortableHint is a resource hint without a portable option representation.
type nonPortableHint struct{}

func (nonPortableHint) URN() string                                    { return "beam:resources:not_portable:v1" }
func (nonPortableHint) Payload() []byte                                { return []byte("value") }
func (h nonPortableHint) MergeWithOuter(_ resource.Hint) resource.Hint { return h }

func mustStruct(t *testing.T, m map[string]any) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(m)
	if err != nil {
		t.Fatalf("structpb.NewStruct(%v) = %v", m, err)
	}
	return s
}

func TestExpansionPipelineOptions(t *testing.T) {
	tests := []struct {
		name  string
		hints resource.Hints
		want  map[string]any // nil indicates no options are expected.
	}{
		{
			name: "noHints",
		}, {
			name:  "onlyNonPortableHints",
			hints: resource.NewHints(nonPortableHint{}),
		}, {
			name:  "standardHints",
			hints: resource.NewHints(resource.ParseMinRAM("16GB"), resource.Accelerator("type:nvidia-l4;count:1"), resource.CPUCount(4), resource.MaxActiveBundlesPerWorker(2)),
			want: map[string]any{
				resourceHintsOptionURN: []any{
					"accelerator=type:nvidia-l4;count:1",
					"cpu_count=4",
					"max_active_bundles_per_worker=2",
					"min_ram=16000000000B",
				},
			},
		}, {
			name:  "nonPortableHintsDropped",
			hints: resource.NewHints(resource.CPUCount(4), nonPortableHint{}),
			want: map[string]any{
				resourceHintsOptionURN: []any{"cpu_count=4"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := expansionPipelineOptions(context.Background(), test.hints)
			if err != nil {
				t.Fatalf("expansionPipelineOptions(%v) error = %v", test.hints, err)
			}
			if test.want == nil {
				if got != nil {
					t.Errorf("expansionPipelineOptions(%v) = %v, want nil", test.hints, got)
				}
				return
			}
			if d := cmp.Diff(mustStruct(t, test.want), got, protocmp.Transform()); d != "" {
				t.Errorf("expansionPipelineOptions(%v) diff (-want, +got):\n%v", test.hints, d)
			}
		})
	}
}

func TestExpand_SetsPipelineOptions(t *testing.T) {
	// Swap in a fresh registry so the test handler doesn't leak to other tests.
	oldReg := defaultReg
	defaultReg = newRegistry()
	defer func() { defaultReg = oldReg }()

	const ns = "capturepipelineoptions"
	var gotReq *jobpb.ExpansionRequest
	if err := defaultReg.RegisterHandler(ns, func(_ context.Context, p *HandlerParams) (*jobpb.ExpansionResponse, error) {
		gotReq = p.Req
		return &jobpb.ExpansionResponse{}, nil
	}); err != nil {
		t.Fatalf("RegisterHandler(%q) = %v", ns, err)
	}

	tests := []struct {
		name  string
		hints resource.Hints
		want  *structpb.Struct
	}{
		{
			name: "noHints",
		}, {
			name:  "withHints",
			hints: resource.NewHints(resource.ParseMinRAM("2GB")),
			want: mustStruct(t, map[string]any{
				resourceHintsOptionURN: []any{"min_ram=2000000000B"},
			}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotReq = nil
			opts, err := expansionPipelineOptions(context.Background(), test.hints)
			if err != nil {
				t.Fatalf("expansionPipelineOptions(%v) error = %v", test.hints, err)
			}
			ext := &graph.ExternalTransform{ExpansionAddr: ns, Namespace: "test"}
			edge := &graph.MultiEdge{External: ext}
			transform := &pipepb.PTransform{Spec: &pipepb.FunctionSpec{Urn: "beam:transform:test:v1"}}

			if _, err := expand(context.Background(), &pipepb.Components{}, transform, edge, ext, opts); err != nil {
				t.Fatalf("expand() error = %v", err)
			}
			if gotReq == nil {
				t.Fatal("expand() didn't call the registered handler")
			}
			if d := cmp.Diff(test.want, gotReq.GetPipelineOptions(), protocmp.Transform()); d != "" {
				t.Errorf("ExpansionRequest.PipelineOptions diff (-want, +got):\n%v", d)
			}
		})
	}
}
