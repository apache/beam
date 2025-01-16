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

	combineTransform := pipepb.PTransform_builder{
		UniqueName: "COMBINE",
		Spec: pipepb.FunctionSpec_builder{
			Urn: urns.TransformCombinePerKey,
			Payload: protox.MustEncode(pipepb.CombinePayload_builder{
				CombineFn: pipepb.FunctionSpec_builder{
					Urn:     "foo",
					Payload: []byte("bar"),
				}.Build(),
				AccumulatorCoderId: "AccumID",
			}.Build()),
		}.Build(),
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
	}.Build()
	combineValuesTransform := pipepb.PTransform_builder{
		UniqueName: "combine_values",
		Subtransforms: []string{
			"bonus_leaf",
		},
	}.Build()
	basePCollectionMap := map[string]*pipepb.PCollection{
		"combineIn": pipepb.PCollection_builder{
			CoderId: "inputCoder",
		}.Build(),
		"combineOut": pipepb.PCollection_builder{
			CoderId: "outputCoder",
		}.Build(),
	}
	baseCoderMap := map[string]*pipepb.Coder{
		"int": pipepb.Coder_builder{
			Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderVarInt}.Build(),
		}.Build(),
		"string": pipepb.Coder_builder{
			Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderStringUTF8}.Build(),
		}.Build(),
		"AccumID": pipepb.Coder_builder{
			Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBytes}.Build(),
		}.Build(),
		"inputCoder": pipepb.Coder_builder{
			Spec:              pipepb.FunctionSpec_builder{Urn: urns.CoderKV}.Build(),
			ComponentCoderIds: []string{"int", "int"},
		}.Build(),
		"outputCoder": pipepb.Coder_builder{
			Spec:              pipepb.FunctionSpec_builder{Urn: urns.CoderKV}.Build(),
			ComponentCoderIds: []string{"int", "string"},
		}.Build(),
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
			comps: pipepb.Components_builder{
				Transforms: map[string]*pipepb.PTransform{
					undertest:        combineTransform,
					"combine_values": combineValuesTransform,
				},
				Pcollections: basePCollectionMap,
				Coders:       baseCoderMap,
			}.Build(),
			want: prepareResult{
				SubbedComps: pipepb.Components_builder{
					Transforms: map[string]*pipepb.PTransform{
						undertest: combineTransform,
					},
				}.Build(),
			},
		}, {
			name:   "lifted",
			lifted: true,
			comps: pipepb.Components_builder{
				Transforms: map[string]*pipepb.PTransform{
					undertest:        combineTransform,
					"combine_values": combineValuesTransform,
				},
				Pcollections: basePCollectionMap,
				Coders:       baseCoderMap,
			}.Build(),
			want: prepareResult{
				RemovedLeaves: []string{"gbk", "combine_values", "bonus_leaf"},
				SubbedComps: pipepb.Components_builder{
					Coders: map[string]*pipepb.Coder{
						"cUnderTest_iter_AccumID": pipepb.Coder_builder{
							Spec:              pipepb.FunctionSpec_builder{Urn: urns.CoderIterable}.Build(),
							ComponentCoderIds: []string{"AccumID"},
						}.Build(),
						"cUnderTest_kv_int_AccumID": pipepb.Coder_builder{
							Spec:              pipepb.FunctionSpec_builder{Urn: urns.CoderKV}.Build(),
							ComponentCoderIds: []string{"int", "AccumID"},
						}.Build(),
						"cUnderTest_kv_int_iterAccumID": pipepb.Coder_builder{
							Spec:              pipepb.FunctionSpec_builder{Urn: urns.CoderKV}.Build(),
							ComponentCoderIds: []string{"int", "cUnderTest_iter_AccumID"},
						}.Build(),
					},
					Pcollections: map[string]*pipepb.PCollection{
						"nUnderTest_lifted": pipepb.PCollection_builder{
							UniqueName: "nUnderTest_lifted",
							CoderId:    "cUnderTest_kv_int_AccumID",
						}.Build(),
						"nUnderTest_grouped": pipepb.PCollection_builder{
							UniqueName: "nUnderTest_grouped",
							CoderId:    "cUnderTest_kv_int_iterAccumID",
						}.Build(),
						"nUnderTest_merged": pipepb.PCollection_builder{
							UniqueName: "nUnderTest_merged",
							CoderId:    "cUnderTest_kv_int_AccumID",
						}.Build(),
					},
					Transforms: map[string]*pipepb.PTransform{
						"eUnderTest_lift": pipepb.PTransform_builder{
							UniqueName: "eUnderTest_lift",
							Spec: pipepb.FunctionSpec_builder{
								Urn:     urns.TransformPreCombine,
								Payload: combineTransform.GetSpec().GetPayload(),
							}.Build(),
							Inputs:  map[string]string{"i0": "combineIn"},
							Outputs: map[string]string{"i0": "nUnderTest_lifted"},
						}.Build(),
						"eUnderTest_gbk": pipepb.PTransform_builder{
							UniqueName: "eUnderTest_gbk",
							Spec: pipepb.FunctionSpec_builder{
								Urn: urns.TransformGBK,
								// Technically shouldn't be here, but since GBK execution doesn't escape the runner
								// this never gets examined.
								Payload: combineTransform.GetSpec().GetPayload(),
							}.Build(),
							Inputs:  map[string]string{"i0": "nUnderTest_lifted"},
							Outputs: map[string]string{"i0": "nUnderTest_grouped"},
						}.Build(),
						"eUnderTest_merge": pipepb.PTransform_builder{
							UniqueName: "eUnderTest_merge",
							Spec: pipepb.FunctionSpec_builder{
								Urn:     urns.TransformMerge,
								Payload: combineTransform.GetSpec().GetPayload(),
							}.Build(),
							Inputs:  map[string]string{"i0": "nUnderTest_grouped"},
							Outputs: map[string]string{"i0": "nUnderTest_merged"},
						}.Build(),
						"eUnderTest_extract": pipepb.PTransform_builder{
							UniqueName: "eUnderTest_extract",
							Spec: pipepb.FunctionSpec_builder{
								Urn:     urns.TransformExtract,
								Payload: combineTransform.GetSpec().GetPayload(),
							}.Build(),
							Inputs:  map[string]string{"i0": "nUnderTest_merged"},
							Outputs: map[string]string{"i0": "combineOut"},
						}.Build(),
					},
				}.Build(),
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
