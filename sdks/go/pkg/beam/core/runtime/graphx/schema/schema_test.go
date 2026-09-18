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

//lint:file-ignore U1000 unused functions/types are for tests

package schema

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/prototext"
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

type myInt int

type anotherStruct struct {
	Q myInt
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

type Exported struct {
	G myInt
}

type hasEmbedded struct {
	Exported
}

type hasEmbeddedPtr struct {
	*Exported
}

type hasMap struct {
	Cypher map[bool]float32 `beam:"cypher"`
}

type nonRegisteredLogical struct {
	k int32
}

// parameterizedString is a logical type with an argument. The tests register
// it with the argument int32(5).
type parameterizedString string

// parameterizedRow is a logical type with an argument and a row storage type,
// so it can be used as a top level type.
type parameterizedRow struct {
	S int64
}

// anyString is a logical type registered without an argument.
type anyString string

var (
	parameterizedStringType = reflect.TypeOf((*parameterizedString)(nil)).Elem()
	parameterizedRowType    = reflect.TypeOf((*parameterizedRow)(nil)).Elem()
	anyStringType           = reflect.TypeOf((*anyString)(nil)).Elem()

	unexportedFieldsType     = reflect.TypeOf((*unexportedFields)(nil)).Elem()
	exportedFuncType         = reflect.TypeOf((*exportedFunc)(nil))
	anotherType              = reflect.TypeOf((*anotherStruct)(nil)).Elem()
	exportedType             = reflect.TypeOf((*Exported)(nil)).Elem()
	hasEmbeddedType          = reflect.TypeOf((*hasEmbedded)(nil)).Elem()
	hasEmbeddedPtrType       = reflect.TypeOf((*hasEmbeddedPtr)(nil)).Elem()
	hasMapType               = reflect.TypeOf((*hasMap)(nil)).Elem()
	nonRegisteredLogicalType = reflect.TypeOf((*nonRegisteredLogical)(nil)).Elem()
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
			rt: hasMapType,
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
				Options: []*pipepb.Option{optGoNillable()},
			},
			rt: reflect.TypeOf(&struct {
				SuperNES int16
			}{}),
		}, {
			st: &pipepb.Schema{
				Options: []*pipepb.Option{
					logicalOption("schema.unexportedFields"),
				},
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
			rt: unexportedFieldsType,
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
						Name: "H",
						Type: &pipepb.FieldType{
							Nullable: true,
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
			rt: reflect.TypeOf(struct{ H *unexportedFields }{}),
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
				Options: []*pipepb.Option{optGoNillable(), logicalOption("*schema.exportedFunc")},
			},
			rt: exportedFuncType,
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					{
						Name: "Q",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_LogicalType{
								LogicalType: &pipepb.LogicalType{
									Urn: "schema.myInt",
									Representation: &pipepb.FieldType{
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
			rt: anotherType,
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					{
						Name:    "Exported",
						Options: []*pipepb.Option{optGoEmbedded()},
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_RowType{
								RowType: &pipepb.RowType{
									Schema: &pipepb.Schema{
										Fields: []*pipepb.Field{
											{
												Name: "G",
												Type: &pipepb.FieldType{
													TypeInfo: &pipepb.FieldType_LogicalType{
														LogicalType: &pipepb.LogicalType{
															Urn: "schema.myInt",
															Representation: &pipepb.FieldType{
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
								},
							},
						},
					},
				},
			},
			rt: hasEmbeddedType,
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					{
						Name:    "Exported",
						Options: []*pipepb.Option{optGoEmbedded()},
						Type: &pipepb.FieldType{
							Nullable: true,
							TypeInfo: &pipepb.FieldType_RowType{
								RowType: &pipepb.RowType{
									Schema: &pipepb.Schema{
										Fields: []*pipepb.Field{
											{
												Name: "G",
												Type: &pipepb.FieldType{
													TypeInfo: &pipepb.FieldType_LogicalType{
														LogicalType: &pipepb.LogicalType{
															Urn: "schema.myInt",
															Representation: &pipepb.FieldType{
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
								},
							},
						},
					},
				},
			},
			rt: hasEmbeddedPtrType,
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					{
						Name: "T",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_AtomicType{
								AtomicType: pipepb.AtomicType_STRING,
							},
						},
					},
				},
				Options: []*pipepb.Option{optGoNillable()},
			},
			rt: reflect.TypeOf(&struct {
				myInt
				T string
				i int
			}{}),
		}, {
			st: &pipepb.Schema{
				Options: []*pipepb.Option{
					logicalOption("schema.exportedFunc"),
				},
				Fields: []*pipepb.Field{
					{
						Name: "V",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_AtomicType{
								AtomicType: pipepb.AtomicType_INT16,
							},
						},
					},
				},
			},
			rt: reflect.TypeOf(exportedFunc{}),
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					{
						Name: "U",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_LogicalType{
								LogicalType: &pipepb.LogicalType{
									Urn: "schema.exportedFunc",
									Representation: &pipepb.FieldType{
										TypeInfo: &pipepb.FieldType_RowType{
											RowType: &pipepb.RowType{
												Schema: &pipepb.Schema{
													Fields: []*pipepb.Field{
														{
															Name: "V",
															Type: &pipepb.FieldType{
																TypeInfo: &pipepb.FieldType_AtomicType{
																	AtomicType: pipepb.AtomicType_INT16,
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
			rt: reflect.TypeOf(struct {
				U exportedFunc
			}{}),
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					{
						Name: "U",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_LogicalType{
								LogicalType: &pipepb.LogicalType{
									Urn: "schema.nonRegisteredLogical",
									Representation: &pipepb.FieldType{
										TypeInfo: &pipepb.FieldType_RowType{
											RowType: &pipepb.RowType{
												Schema: &pipepb.Schema{
													Fields: []*pipepb.Field{
														{
															Name: "K",
															Type: &pipepb.FieldType{
																TypeInfo: &pipepb.FieldType_AtomicType{
																	AtomicType: pipepb.AtomicType_INT32,
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
			rt: reflect.TypeOf(struct {
				U nonRegisteredLogical
			}{}),
		}, {
			st: &pipepb.Schema{
				Fields: []*pipepb.Field{
					{
						Name: "P",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_LogicalType{
								LogicalType: &pipepb.LogicalType{
									Urn: "schema.parameterizedString",
									Representation: &pipepb.FieldType{
										TypeInfo: &pipepb.FieldType_AtomicType{
											AtomicType: pipepb.AtomicType_STRING,
										},
									},
									ArgumentType: int32FieldType(),
									Argument:     int32FieldValue(5),
								},
							},
						},
					},
				},
			},
			rt: reflect.TypeOf(struct {
				P parameterizedString
			}{}),
		}, {
			st: &pipepb.Schema{
				Options: []*pipepb.Option{
					logicalOption("schema.parameterizedRow"),
					{
						Name:  optGoLogicalArgumentUrn,
						Type:  int32FieldType(),
						Value: int32FieldValue(3),
					},
				},
				Fields: []*pipepb.Field{
					{
						Name: "S",
						Type: &pipepb.FieldType{
							TypeInfo: &pipepb.FieldType_AtomicType{
								AtomicType: pipepb.AtomicType_INT64,
							},
						},
					},
				},
			},
			rt: parameterizedRowType,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("%v", test.rt), func(t *testing.T) {
			reg := NewRegistry()
			preRegLogicalTypes(reg)
			reg.RegisterLogicalType(ToLogicalType(exportedFuncType.Elem().String(), exportedFuncType.Elem(), reflect.TypeOf(struct{ V int16 }{})))
			reg.RegisterLogicalType(ToLogicalType(nonRegisteredLogicalType.String(), nonRegisteredLogicalType, reflect.TypeOf(struct{ K int32 }{})))
			reg.RegisterLogicalType(ToLogicalTypeWithArgument("schema.parameterizedString", parameterizedStringType, reflectx.String, int32(5)))
			reg.RegisterLogicalType(ToLogicalTypeWithArgument("schema.parameterizedRow", parameterizedRowType, reflect.TypeOf(struct{ S int64 }{}), int32(3)))
			reg.RegisterType(reflect.TypeOf((*sRegisteredType)(nil)))
			reg.RegisterLogicalTypeProvider(reflect.TypeOf((*testInterface)(nil)).Elem(), func(t reflect.Type) (reflect.Type, error) {
				switch t {
				case unexportedFieldsType:
					return reflect.TypeOf(struct{ D uint64 }{}), nil
				case exportedFuncType:
					return reflect.TypeOf(struct{ E int16 }{}), nil
				}
				return nil, nil
			})
			reg.RegisterType(unexportedFieldsType)
			reg.RegisterType(exportedFuncType)
			reg.RegisterType(anotherType)
			reg.RegisterType(exportedType)
			reg.RegisterType(hasEmbeddedType)
			reg.RegisterType(hasEmbeddedPtrType)
			reg.RegisterType(hasMapType)

			{
				got, err := reg.ToType(test.st)
				if err != nil {
					t.Fatalf("error ToType(%v) = %v", test.st, err)
				}
				// We can't validate that synthetic types from Schemas with embedded fields are
				// assignable, as the anonymous struct field won't be equivalent to the
				// real embedded type.
				if !hasEmbeddedField(test.rt) && !test.rt.AssignableTo(got) {
					t.Errorf("%v not assignable to %v", test.rt, got)
					t.Errorf("%v for schema %v", test.rt, prototext.Format(test.st))
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
					protocmp.IgnoreFields(&pipepb.Schema{}, "id"),
				); d != "" {
					t.Errorf("diff (-want, +got): %v", d)
				}

			}
		})
	}
}

func hasEmbeddedField(t reflect.Type) bool {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	for i := 0; i < t.NumField(); i++ {
		if t.Field(i).Anonymous {
			return true
		}
	}
	return false
}

func int32FieldType() *pipepb.FieldType {
	return &pipepb.FieldType{
		TypeInfo: &pipepb.FieldType_AtomicType{
			AtomicType: pipepb.AtomicType_INT32,
		},
	}
}

func int32FieldValue(v int32) *pipepb.FieldValue {
	return &pipepb.FieldValue{
		FieldValue: &pipepb.FieldValue_AtomicValue{
			AtomicValue: &pipepb.AtomicTypeValue{
				Value: &pipepb.AtomicTypeValue_Int32{Int32: v},
			},
		},
	}
}

func TestToType_LogicalTypeArguments(t *testing.T) {
	reg := NewRegistry()
	preRegLogicalTypes(reg)
	reg.RegisterLogicalType(ToLogicalTypeWithArgument("schema.parameterizedString", parameterizedStringType, reflectx.String, int32(5)))
	reg.RegisterLogicalType(ToLogicalType("schema.anyString", anyStringType, reflectx.String))

	stringType := &pipepb.FieldType{
		TypeInfo: &pipepb.FieldType_AtomicType{
			AtomicType: pipepb.AtomicType_STRING,
		},
	}
	rowType := &pipepb.FieldType{
		TypeInfo: &pipepb.FieldType_RowType{
			RowType: &pipepb.RowType{
				Schema: &pipepb.Schema{
					Fields: []*pipepb.Field{{Name: "precision", Type: int32FieldType()}},
				},
			},
		},
	}
	rowValue := &pipepb.FieldValue{
		FieldValue: &pipepb.FieldValue_RowValue{
			RowValue: &pipepb.Row{Values: []*pipepb.FieldValue{int32FieldValue(5)}},
		},
	}

	tests := []struct {
		name    string
		lt      *pipepb.LogicalType
		want    reflect.Type
		wantErr bool
	}{{
		name: "matching argument",
		lt:   &pipepb.LogicalType{Urn: "schema.parameterizedString", Representation: stringType, ArgumentType: int32FieldType(), Argument: int32FieldValue(5)},
		want: parameterizedStringType,
	}, {
		name:    "unknown argument",
		lt:      &pipepb.LogicalType{Urn: "schema.parameterizedString", Representation: stringType, ArgumentType: int32FieldType(), Argument: int32FieldValue(7)},
		wantErr: true,
	}, {
		name:    "missing argument",
		lt:      &pipepb.LogicalType{Urn: "schema.parameterizedString", Representation: stringType},
		wantErr: true,
	}, {
		name: "argument ignored for logical type registered without one",
		lt:   &pipepb.LogicalType{Urn: "schema.anyString", Representation: stringType, ArgumentType: int32FieldType(), Argument: int32FieldValue(7)},
		want: anyStringType,
	}, {
		name: "non atomic argument ignored",
		lt:   &pipepb.LogicalType{Urn: "schema.anyString", Representation: stringType, ArgumentType: rowType, Argument: rowValue},
		want: anyStringType,
	}, {
		name:    "unknown urn",
		lt:      &pipepb.LogicalType{Urn: "schema.unknown", Representation: stringType},
		wantErr: true,
	}}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := &pipepb.Schema{
				Id: fmt.Sprintf("arguments-%d", i),
				Fields: []*pipepb.Field{{
					Name: "f",
					Type: &pipepb.FieldType{
						TypeInfo: &pipepb.FieldType_LogicalType{LogicalType: test.lt},
					},
				}},
			}
			got, err := reg.ToType(s)
			if test.wantErr {
				if err == nil {
					t.Fatalf("ToType(%v) = %v, want error", prototext.Format(s), got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ToType(%v) = %v, want nil error", prototext.Format(s), err)
			}
			if ft := got.Field(0).Type; ft != test.want {
				t.Errorf("ToType(%v) field type = %v, want %v", prototext.Format(s), ft, test.want)
			}
		})
	}
}

// The multi types share one URN with different arguments, and multiAny is
// registered for the URN without an argument.
type multiOne struct{ S int64 }
type multiTwo struct{ S int64 }
type multiAny struct{ S int64 }

func TestLogicalTypeArgumentPrecedence(t *testing.T) {
	reg := NewRegistry()
	preRegLogicalTypes(reg)
	storage := reflect.TypeOf(struct{ S int64 }{})
	oneType := reflect.TypeOf(multiOne{})
	twoType := reflect.TypeOf(multiTwo{})
	anyType := reflect.TypeOf(multiAny{})
	reg.RegisterLogicalType(ToLogicalTypeWithArgument("schema.multi", oneType, storage, int32(1)))
	reg.RegisterLogicalType(ToLogicalTypeWithArgument("schema.multi", twoType, storage, int32(2)))
	reg.RegisterLogicalType(ToLogicalType("schema.multi", anyType, storage))

	rowType := &pipepb.FieldType{
		TypeInfo: &pipepb.FieldType_RowType{RowType: &pipepb.RowType{Schema: &pipepb.Schema{
			Fields: []*pipepb.Field{{Name: "S", Type: &pipepb.FieldType{
				TypeInfo: &pipepb.FieldType_AtomicType{AtomicType: pipepb.AtomicType_INT64},
			}}},
		}}},
	}
	fieldSchema := func(id string, argument *pipepb.FieldValue) *pipepb.Schema {
		lt := &pipepb.LogicalType{Urn: "schema.multi", Representation: rowType}
		if argument != nil {
			lt.ArgumentType = int32FieldType()
			lt.Argument = argument
		}
		return &pipepb.Schema{Id: id, Fields: []*pipepb.Field{{Name: "f", Type: &pipepb.FieldType{
			TypeInfo: &pipepb.FieldType_LogicalType{LogicalType: lt},
		}}}}
	}
	topLevelSchema := func(id string, argument *pipepb.FieldValue) *pipepb.Schema {
		opts := []*pipepb.Option{logicalOption("schema.multi")}
		if argument != nil {
			opts = append(opts, &pipepb.Option{Name: optGoLogicalArgumentUrn, Type: int32FieldType(), Value: argument})
		}
		return &pipepb.Schema{Id: id, Options: opts, Fields: rowType.GetRowType().GetSchema().GetFields()}
	}

	tests := []struct {
		name string
		s    *pipepb.Schema
		want reflect.Type
	}{
		{"field argument 1", fieldSchema("field-1", int32FieldValue(1)), oneType},
		{"field argument 2", fieldSchema("field-2", int32FieldValue(2)), twoType},
		{"field argument 3 falls back", fieldSchema("field-3", int32FieldValue(3)), anyType},
		{"field without argument", fieldSchema("field-none", nil), anyType},
		{"top level argument 2", topLevelSchema("top-2", int32FieldValue(2)), twoType},
		{"top level argument 3 falls back", topLevelSchema("top-3", int32FieldValue(3)), anyType},
		{"top level without argument", topLevelSchema("top-none", nil), anyType},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := reg.ToType(test.s)
			if err != nil {
				t.Fatalf("ToType(%v) = %v, want nil error", prototext.Format(test.s), err)
			}
			if len(test.s.GetOptions()) == 0 {
				got = got.Field(0).Type
			}
			if got != test.want {
				t.Errorf("ToType(%v) = %v, want exactly %v", prototext.Format(test.s), got, test.want)
			}
		})
	}
	for _, rt := range []reflect.Type{oneType, twoType, anyType} {
		schm, err := reg.FromType(rt)
		if err != nil {
			t.Fatalf("FromType(%v) = %v, want nil error", rt, err)
		}
		got, err := reg.ToType(schm)
		if err != nil {
			t.Fatalf("ToType(FromType(%v)) = %v, want nil error", rt, err)
		}
		if got != rt {
			t.Errorf("ToType(FromType(%v)) = %v, want exactly %v", rt, got, rt)
		}
	}
}

type bytesBacked struct{ B []byte }

func TestRegisterLogicalType_Validation(t *testing.T) {
	reg := NewRegistry()
	preRegLogicalTypes(reg)
	bytesBackedType := reflect.TypeOf(bytesBacked{})
	reg.RegisterLogicalType(ToLogicalType("schema.bytesBacked", bytesBackedType, reflectx.ByteSlice))
	if schm, err := reg.FromType(bytesBackedType); err == nil {
		t.Errorf("FromType(%v) = %v, want error for a top level logical type with non row storage", bytesBackedType, prototext.Format(schm))
	}
	if _, err := reg.FromType(reflect.TypeOf(struct{ F bytesBacked }{})); err != nil {
		t.Errorf("FromType(struct{F bytesBacked}) = %v, want nil error", err)
	}

	for name, argument := range map[string]any{
		"NaN":         math.NaN(),
		"float32 NaN": float32(math.NaN()),
		"[]byte":      []byte("x"),
		"int":         int(1),
		"struct":      struct{ A int32 }{},
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("RegisterLogicalType with argument %v did not panic", argument)
				}
			}()
			reg.RegisterLogicalType(ToLogicalTypeWithArgument("schema.invalid", anyStringType, reflectx.String, argument))
		})
	}
}
