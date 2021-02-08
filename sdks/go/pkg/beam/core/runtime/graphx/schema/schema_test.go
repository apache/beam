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

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

type registeredType struct {
	A, B string
	C    bool
}

type sRegisteredType struct {
	D int32
}

type justAType struct {
	A, B string
	C    int
}

func init() {
	runtime.RegisterType(reflect.TypeOf((*registeredType)(nil)))
}

type testInterface interface {
	hidden()
}

type unexportedFields struct {
	d uint64
}

func (unexportedFields) hidden() {}

type exportedFunc struct {
	e int16
	F func()
}

func (*exportedFunc) hidden() {}

var (
	unexType   = reflect.TypeOf((*unexportedFields)(nil)).Elem()
	exFuncType = reflect.TypeOf((*exportedFunc)(nil))
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
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					&pipepb.Field{
						Name: "AString",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_AtomicType{
								AtomicType: pipepb.AtomicType_STRING,
							},
						},
					},
					&pipepb.Field{
						Name: "AnIntPtr",
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
				AString  string
				AnIntPtr *int32
			}{}),
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					&pipepb.Field{
						Name: "A",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_AtomicType{
								AtomicType: pipepb.AtomicType_STRING,
							},
						},
					},
					&pipepb.Field{
						Name: "B",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_AtomicType{
								AtomicType: pipepb.AtomicType_STRING,
							},
						},
					},
					&pipepb.Field{
						Name: "C",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_AtomicType{
								AtomicType: pipepb.AtomicType_BOOLEAN,
							},
						},
					},
				},
			},
			rt: reflect.TypeOf(registeredType{}),
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					&pipepb.Field{
						Name: "D",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_AtomicType{
								AtomicType: pipepb.AtomicType_INT32,
							},
						},
					},
				},
			},
			rt: reflect.TypeOf(sRegisteredType{}),
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					&pipepb.Field{
						Name: "A",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_AtomicType{
								AtomicType: pipepb.AtomicType_STRING,
							},
						},
					},
					&pipepb.Field{
						Name: "B",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_AtomicType{
								AtomicType: pipepb.AtomicType_STRING,
							},
						},
					},
					&pipepb.Field{
						Name: "C",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_LogicalType{
								LogicalType: &pipepb.LogicalType{
									Urn: "int",
									Representation: &pipepb.FieldType{
										TypeInfo: &pipepb.FieldType_AtomicType{
											AtomicType: pipepb.AtomicType_INT64,
										},
									},
								},
							},
						},
					},
				},
			},
			rt: reflect.TypeOf(justAType{}),
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					{
						Name: "Q",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_MapType{
								MapType: &pipepb.MapType{
									KeyType: &pipepb.FieldType{
										TypeInfo: &pipepb.FieldType_LogicalType{
											LogicalType: &pipepb.LogicalType{
												Urn: "int",
												Representation: &pipepb.FieldType{
													TypeInfo: &pipepb.FieldType_AtomicType{
														AtomicType: pipepb.AtomicType_INT64,
													},
												},
											},
										},
									},
									ValueType: &pipepb.FieldType{
										TypeInfo: &pipepb.FieldType_LogicalType{
											LogicalType: &pipepb.LogicalType{
												Urn: "int",
												Representation: &pipepb.FieldType{
													TypeInfo: &pipepb.FieldType_AtomicType{
														AtomicType: pipepb.AtomicType_INT64,
													},
												},
											},
										},
									},
								},
							},
						},
					}, {
						Name: "T",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_ArrayType{
								ArrayType: &pipepb.ArrayType{
									ElementType: &pipepb.FieldType{
										TypeInfo: &pipepb.FieldType_LogicalType{
											LogicalType: &pipepb.LogicalType{
												Urn: "int",
												Representation: &pipepb.FieldType{
													TypeInfo: &pipepb.FieldType_AtomicType{
														AtomicType: pipepb.AtomicType_INT64,
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
				Q map[int]int
				T []int
			}{}),
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					{
						Name: "SuperNES",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_AtomicType{
								AtomicType: pipepb.AtomicType_INT16,
							},
						},
					},
				},
				Options: []*pipepb.Option{{
					Name: optGoNillable,
				}},
			},
			rt: reflect.TypeOf(&struct {
				SuperNES int16
			}{}),
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					{
						Name: "D",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_LogicalType{
								LogicalType: &pipepb.LogicalType{
									Urn: "uint64",
									Representation: &pipepb.FieldType{
										TypeInfo: &pipepb.FieldType_AtomicType{
											AtomicType: pipepb.AtomicType_INT64,
										},
									},
								},
							},
						},
					},
				},
			},
			rt: unexType,
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					{
						Name: "G",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_LogicalType{
								LogicalType: &pipepb.LogicalType{
									Urn: "schema.unexportedFields",
									Representation: &pipepb.FieldType{
										TypeInfo: &pipepb.FieldType_RowType{
											RowType: &pipepb.RowType{
												Schema: &pipepb.Schema{
													Fields: []*pipepb.Field{
														{
															Name: "D",
															Type: &pipepb.FieldType{
																TypeInfo: &pipepb.FieldType_LogicalType{
																	LogicalType: &pipepb.LogicalType{
																		Urn: "uint64",
																		Representation: &pipepb.FieldType{
																			TypeInfo: &pipepb.FieldType_AtomicType{
																				AtomicType: pipepb.AtomicType_INT64,
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
								},
							},
						},
					},
				},
			},
			rt: reflect.TypeOf(struct{ G unexportedFields }{}),
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					{
						Name: "E",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_AtomicType{
								AtomicType: pipepb.AtomicType_INT16,
							},
						},
					},
				},
				Options: []*pipepb.Option{{
					Name: optGoNillable,
				}},
			},
			rt: exFuncType,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("%v", test.rt), func(t *testing.T) {
			reg := NewRegistry()
			preRegLogicalTypes(reg)
			reg.RegisterType(reflect.TypeOf((*sRegisteredType)(nil)))
			reg.RegisterLogicalTypeProvider(reflect.TypeOf((*testInterface)(nil)).Elem(), func(t reflect.Type) (reflect.Type, error) {
				switch t {
				case unexType:
					return reflect.TypeOf(struct{ D uint64 }{}), nil
				case exFuncType:
					return reflect.TypeOf(struct{ E int16 }{}), nil
				}
				return nil, nil
			})
			reg.RegisterType(unexType)
			reg.RegisterType(exFuncType)

			{
				got, err := reg.ToType(test.st)
				if err != nil {
					t.Fatalf("error ToType(%v) = %v", test.st, err)
				}
				if !test.rt.AssignableTo(got) {
					t.Errorf("%v not assignable to %v", test.rt, got)
					if d := cmp.Diff(reflect.New(test.rt).Elem().Interface(), reflect.New(got).Elem().Interface()); d != "" {
						t.Errorf("diff (-want, +got): %v", d)
					}
				}
			}
			{
				got, err := reg.FromType(test.rt)
				if err != nil {
					t.Fatalf("error FromType(%v) = %v", test.rt, err)
				}
				if d := cmp.Diff(test.st, got,
					protocmp.Transform(),
					protocmp.IgnoreFields(proto.MessageV2(&pipepb.Schema{}), "id"),
				); d != "" {
					t.Errorf("diff (-want, +got): %v", d)
				}

			}
		})
	}
}
