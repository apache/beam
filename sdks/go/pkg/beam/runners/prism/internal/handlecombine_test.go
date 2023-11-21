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
	"google.golang.org/protobuf/testing/protocmp"
)

func TestHandleCombine(t *testing.T) {
	undertest := "UnderTest"

	combineTransform := &pipepb.PTransform{
		UniqueName: "COMBINE",
		Spec: &pipepb.FunctionSpec{
			Urn: urns.TransformCombinePerKey,
			Payload: protox.MustEncode(&pipepb.CombinePayload{
				CombineFn: &pipepb.FunctionSpec{
					Urn:     "foo",
					Payload: []byte("bar"),
				},
				AccumulatorCoderId: "AccumID",
			}),
		},
		Inputs: map[string]string{
			"input": "combineIn",
		},
		Outputs: map[string]string{
			"input": "combineOut",
		},
		Subtransforms: []string{
			"gbk",
			"combine_values",
		},
	}
	combineValuesTransform := &pipepb.PTransform{
		UniqueName: "combine_values",
		Subtransforms: []string{
			"bonus_leaf",
		},
	}
	basePCollectionMap := map[string]*pipepb.PCollection{
		"combineIn": {
			CoderId: "inputCoder",
		},
		"combineOut": {
			CoderId: "outputCoder",
		},
	}
	baseCoderMap := map[string]*pipepb.Coder{
		"int": {
			Spec: &pipepb.FunctionSpec{Urn: urns.CoderVarInt},
		},
		"string": {
			Spec: &pipepb.FunctionSpec{Urn: urns.CoderStringUTF8},
		},
		"AccumID": {
			Spec: &pipepb.FunctionSpec{Urn: urns.CoderBytes},
		},
		"inputCoder": {
			Spec:              &pipepb.FunctionSpec{Urn: urns.CoderKV},
			ComponentCoderIds: []string{"int", "int"},
		},
		"outputCoder": {
			Spec:              &pipepb.FunctionSpec{Urn: urns.CoderKV},
			ComponentCoderIds: []string{"int", "string"},
		},
	}

	tests := []struct {
		name   string
		lifted bool
		comps  *pipepb.Components

		want prepareResult
	}{
		{
			name:   "unlifted",
			lifted: false,
			comps: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					undertest:        combineTransform,
					"combine_values": combineValuesTransform,
				},
				Pcollections: basePCollectionMap,
				Coders:       baseCoderMap,
			},
			want: prepareResult{
				SubbedComps: &pipepb.Components{
					Transforms: map[string]*pipepb.PTransform{
						undertest: combineTransform,
					},
				},
			},
		}, {
			name:   "lifted",
			lifted: true,
			comps: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					undertest:        combineTransform,
					"combine_values": combineValuesTransform,
				},
				Pcollections: basePCollectionMap,
				Coders:       baseCoderMap,
			},
			want: prepareResult{
				RemovedLeaves: []string{"gbk", "combine_values", "bonus_leaf"},
				SubbedComps: &pipepb.Components{
					Coders: map[string]*pipepb.Coder{
						"cUnderTest_iter_AccumID": {
							Spec:              &pipepb.FunctionSpec{Urn: urns.CoderIterable},
							ComponentCoderIds: []string{"AccumID"},
						},
						"cUnderTest_kv_int_AccumID": {
							Spec:              &pipepb.FunctionSpec{Urn: urns.CoderKV},
							ComponentCoderIds: []string{"int", "AccumID"},
						},
						"cUnderTest_kv_int_iterAccumID": {
							Spec:              &pipepb.FunctionSpec{Urn: urns.CoderKV},
							ComponentCoderIds: []string{"int", "cUnderTest_iter_AccumID"},
						},
					},
					Pcollections: map[string]*pipepb.PCollection{
						"nUnderTest_lifted": {
							UniqueName: "nUnderTest_lifted",
							CoderId:    "cUnderTest_kv_int_AccumID",
						},
						"nUnderTest_grouped": {
							UniqueName: "nUnderTest_grouped",
							CoderId:    "cUnderTest_kv_int_iterAccumID",
						},
						"nUnderTest_merged": {
							UniqueName: "nUnderTest_merged",
							CoderId:    "cUnderTest_kv_int_AccumID",
						},
					},
					Transforms: map[string]*pipepb.PTransform{
						"eUnderTest_lift": {
							UniqueName: "eUnderTest_lift",
							Spec: &pipepb.FunctionSpec{
								Urn:     urns.TransformPreCombine,
								Payload: combineTransform.GetSpec().GetPayload(),
							},
							Inputs:  map[string]string{"i0": "combineIn"},
							Outputs: map[string]string{"i0": "nUnderTest_lifted"},
						},
						"eUnderTest_gbk": {
							UniqueName: "eUnderTest_gbk",
							Spec: &pipepb.FunctionSpec{
								Urn: urns.TransformGBK,
								// Technically shouldn't be here, but since GBK execution doesn't escape the runner
								// this never gets examined.
								Payload: combineTransform.GetSpec().GetPayload(),
							},
							Inputs:  map[string]string{"i0": "nUnderTest_lifted"},
							Outputs: map[string]string{"i0": "nUnderTest_grouped"},
						},
						"eUnderTest_merge": {
							UniqueName: "eUnderTest_merge",
							Spec: &pipepb.FunctionSpec{
								Urn:     urns.TransformMerge,
								Payload: combineTransform.GetSpec().GetPayload(),
							},
							Inputs:  map[string]string{"i0": "nUnderTest_grouped"},
							Outputs: map[string]string{"i0": "nUnderTest_merged"},
						},
						"eUnderTest_extract": {
							UniqueName: "eUnderTest_extract",
							Spec: &pipepb.FunctionSpec{
								Urn:     urns.TransformExtract,
								Payload: combineTransform.GetSpec().GetPayload(),
							},
							Inputs:  map[string]string{"i0": "nUnderTest_merged"},
							Outputs: map[string]string{"i0": "combineOut"},
						},
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := combine{
				config: CombineCharacteristic{
					EnableLifting: test.lifted,
				},
			}
			got := handler.PrepareTransform(undertest, test.comps.GetTransforms()[undertest], test.comps)
			if d := cmp.Diff(test.want, got, protocmp.Transform()); d != "" {
				t.Errorf("combine.PrepareTransform diff (-want, +got):\n%v", d)
			}
		})
	}
}
