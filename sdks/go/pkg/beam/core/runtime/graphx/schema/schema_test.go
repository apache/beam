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

package schema

import (
	"fmt"
	"reflect"
	"testing"

	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
)

func TestSchemaConversion(t *testing.T) {
	tests := []struct {
		st *pipepb.Schema
		rt reflect.Type
	}{
		{
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					&pipepb.Field{
						Name: "firstField",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_AtomicType{
								AtomicType: pipepb.AtomicType_INT32,
							},
						},
					},
				},
			},
			rt: reflect.TypeOf(struct {
				FirstField int32 `beam:"firstField"`
			}{}),
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					&pipepb.Field{
						Name: "stringField",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_AtomicType{
								AtomicType: pipepb.AtomicType_STRING,
							},
						},
					},
					&pipepb.Field{
						Name: "intPtrField",
						Type: &pipepb.FieldType{
							Nullable: true,
							TypeInfo: &pipepb.FieldType_AtomicType{
								AtomicType: pipepb.AtomicType_INT32,
							},
						},
					},
				},
			},
			rt: reflect.TypeOf(struct {
				StringField string `beam:"stringField"`
				IntPtrField *int32 `beam:"intPtrField"`
			}{}),
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					&pipepb.Field{
						Name: "cypher",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_MapType{
								MapType: &pipepb.MapType{
									KeyType: &pipepb.FieldType{
										TypeInfo: &pipepb.FieldType_AtomicType{
											AtomicType: pipepb.AtomicType_BOOLEAN,
										},
									},
									ValueType: &pipepb.FieldType{
										TypeInfo: &pipepb.FieldType_AtomicType{
											AtomicType: pipepb.AtomicType_FLOAT,
										},
									},
								},
							},
						},
					},
				},
			},
			rt: reflect.TypeOf(struct {
				Cypher map[bool]float32 `beam:"cypher"`
			}{}),
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					&pipepb.Field{
						Name: "wrapper",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_RowType{
								RowType: &pipepb.RowType{
									Schema: &pipepb.Schema{
										Fields: []*pipepb.Field{{
											Name: "threshold",
											Type: &pipepb.FieldType{
												TypeInfo: &pipepb.FieldType_AtomicType{
													AtomicType: pipepb.AtomicType_DOUBLE,
												},
											},
										},
										},
									},
								},
							},
						},
					},
				},
			},
			rt: reflect.TypeOf(struct {
				Wrapper struct {
					Threshold float64 `beam:"threshold"`
				} `beam:"wrapper"`
			}{}),
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					&pipepb.Field{
						Name: "payloads",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_ArrayType{
								ArrayType: &pipepb.ArrayType{
									ElementType: &pipepb.FieldType{
										TypeInfo: &pipepb.FieldType_AtomicType{
											AtomicType: pipepb.AtomicType_BYTES,
										},
									},
								},
							},
						},
					},
				},
			},
			rt: reflect.TypeOf(struct {
				Payloads [][]byte `beam:"payloads"`
			}{}),
		},
	}

	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("%v", test.rt), func(t *testing.T) {
			{
				got, err := ToType(test.st)
				if err != nil {
					t.Fatalf("error ToType(%v) = %v", test.st, err)
				}

				if d := cmp.Diff(reflect.New(test.rt).Elem().Interface(), reflect.New(got).Elem().Interface()); d != "" {
					t.Errorf("diff (-want, +got): %v", d)
				}
			}
			{
				got, err := FromType(test.rt)
				if err != nil {
					t.Fatalf("error FromType(%v) = %v", test.rt, err)
				}

				if d := cmp.Diff(test.st, got); d != "" {
					t.Errorf("diff (-want, +got): %v", d)
				}

			}
		})
	}
}
