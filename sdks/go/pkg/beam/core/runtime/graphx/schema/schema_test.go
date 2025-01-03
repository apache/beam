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
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
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

var (
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
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "firstField",
						Type: pipepb.FieldType_builder{
							AtomicType: pipepb.AtomicType_INT32.Enum(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			rt: reflect.TypeOf(struct {
				FirstField int32 `beam:"firstField"`
			}{}),
		}, {
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "stringField",
						Type: pipepb.FieldType_builder{
							AtomicType: pipepb.AtomicType_STRING.Enum(),
						}.Build(),
					}.Build(),
					pipepb.Field_builder{
						Name: "intPtrField",
						Type: pipepb.FieldType_builder{
							Nullable:   true,
							AtomicType: pipepb.AtomicType_INT32.Enum(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			rt: reflect.TypeOf(struct {
				StringField string `beam:"stringField"`
				IntPtrField *int32 `beam:"intPtrField"`
			}{}),
		}, {
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "cypher",
						Type: pipepb.FieldType_builder{
							MapType: pipepb.MapType_builder{
								KeyType: pipepb.FieldType_builder{
									AtomicType: pipepb.AtomicType_BOOLEAN.Enum(),
								}.Build(),
								ValueType: pipepb.FieldType_builder{
									AtomicType: pipepb.AtomicType_FLOAT.Enum(),
								}.Build(),
							}.Build(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			rt: hasMapType,
		}, {
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "wrapper",
						Type: pipepb.FieldType_builder{
							RowType: pipepb.RowType_builder{
								Schema: pipepb.Schema_builder{
									Fields: []*pipepb.Field{pipepb.Field_builder{
										Name: "threshold",
										Type: pipepb.FieldType_builder{
											AtomicType: pipepb.AtomicType_DOUBLE.Enum(),
										}.Build(),
									}.Build(),
									},
								}.Build(),
							}.Build(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			rt: reflect.TypeOf(struct {
				Wrapper struct {
					Threshold float64 `beam:"threshold"`
				} `beam:"wrapper"`
			}{}),
		}, {
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "payloads",
						Type: pipepb.FieldType_builder{
							ArrayType: pipepb.ArrayType_builder{
								ElementType: pipepb.FieldType_builder{
									AtomicType: pipepb.AtomicType_BYTES.Enum(),
								}.Build(),
							}.Build(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			rt: reflect.TypeOf(struct {
				Payloads [][]byte `beam:"payloads"`
			}{}),
		}, {
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "AString",
						Type: pipepb.FieldType_builder{
							AtomicType: pipepb.AtomicType_STRING.Enum(),
						}.Build(),
					}.Build(),
					pipepb.Field_builder{
						Name: "AnIntPtr",
						Type: pipepb.FieldType_builder{
							Nullable:   true,
							AtomicType: pipepb.AtomicType_INT32.Enum(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			rt: reflect.TypeOf(struct {
				AString  string
				AnIntPtr *int32
			}{}),
		}, {
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "A",
						Type: pipepb.FieldType_builder{
							AtomicType: pipepb.AtomicType_STRING.Enum(),
						}.Build(),
					}.Build(),
					pipepb.Field_builder{
						Name: "B",
						Type: pipepb.FieldType_builder{
							AtomicType: pipepb.AtomicType_STRING.Enum(),
						}.Build(),
					}.Build(),
					pipepb.Field_builder{
						Name: "C",
						Type: pipepb.FieldType_builder{
							AtomicType: pipepb.AtomicType_BOOLEAN.Enum(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			rt: reflect.TypeOf(registeredType{}),
		}, {
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "D",
						Type: pipepb.FieldType_builder{
							AtomicType: pipepb.AtomicType_INT32.Enum(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			rt: reflect.TypeOf(sRegisteredType{}),
		}, {
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "A",
						Type: pipepb.FieldType_builder{
							AtomicType: pipepb.AtomicType_STRING.Enum(),
						}.Build(),
					}.Build(),
					pipepb.Field_builder{
						Name: "B",
						Type: pipepb.FieldType_builder{
							AtomicType: pipepb.AtomicType_STRING.Enum(),
						}.Build(),
					}.Build(),
					pipepb.Field_builder{
						Name: "C",
						Type: pipepb.FieldType_builder{
							LogicalType: pipepb.LogicalType_builder{
								Urn: "int",
								Representation: pipepb.FieldType_builder{
									AtomicType: pipepb.AtomicType_INT64.Enum(),
								}.Build(),
							}.Build(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			rt: reflect.TypeOf(justAType{}),
		}, {
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "Q",
						Type: pipepb.FieldType_builder{
							MapType: pipepb.MapType_builder{
								KeyType: pipepb.FieldType_builder{
									LogicalType: pipepb.LogicalType_builder{
										Urn: "int",
										Representation: pipepb.FieldType_builder{
											AtomicType: pipepb.AtomicType_INT64.Enum(),
										}.Build(),
									}.Build(),
								}.Build(),
								ValueType: pipepb.FieldType_builder{
									LogicalType: pipepb.LogicalType_builder{
										Urn: "int",
										Representation: pipepb.FieldType_builder{
											AtomicType: pipepb.AtomicType_INT64.Enum(),
										}.Build(),
									}.Build(),
								}.Build(),
							}.Build(),
						}.Build(),
					}.Build(), pipepb.Field_builder{
						Name: "T",
						Type: pipepb.FieldType_builder{
							ArrayType: pipepb.ArrayType_builder{
								ElementType: pipepb.FieldType_builder{
									LogicalType: pipepb.LogicalType_builder{
										Urn: "int",
										Representation: pipepb.FieldType_builder{
											AtomicType: pipepb.AtomicType_INT64.Enum(),
										}.Build(),
									}.Build(),
								}.Build(),
							}.Build(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			rt: reflect.TypeOf(struct {
				Q map[int]int
				T []int
			}{}),
		}, {
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "SuperNES",
						Type: pipepb.FieldType_builder{
							AtomicType: pipepb.AtomicType_INT16.Enum(),
						}.Build(),
					}.Build(),
				},
				Options: []*pipepb.Option{optGoNillable()},
			}.Build(),
			rt: reflect.TypeOf(&struct {
				SuperNES int16
			}{}),
		}, {
			st: pipepb.Schema_builder{
				Options: []*pipepb.Option{
					logicalOption("schema.unexportedFields"),
				},
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "D",
						Type: pipepb.FieldType_builder{
							LogicalType: pipepb.LogicalType_builder{
								Urn: "uint64",
								Representation: pipepb.FieldType_builder{
									AtomicType: pipepb.AtomicType_INT64.Enum(),
								}.Build(),
							}.Build(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			rt: unexportedFieldsType,
		}, {
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "G",
						Type: pipepb.FieldType_builder{
							LogicalType: pipepb.LogicalType_builder{
								Urn: "schema.unexportedFields",
								Representation: pipepb.FieldType_builder{
									RowType: pipepb.RowType_builder{
										Schema: pipepb.Schema_builder{
											Fields: []*pipepb.Field{
												pipepb.Field_builder{
													Name: "D",
													Type: pipepb.FieldType_builder{
														LogicalType: pipepb.LogicalType_builder{
															Urn: "uint64",
															Representation: pipepb.FieldType_builder{
																AtomicType: pipepb.AtomicType_INT64.Enum(),
															}.Build(),
														}.Build(),
													}.Build(),
												}.Build(),
											},
										}.Build(),
									}.Build(),
								}.Build(),
							}.Build(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			rt: reflect.TypeOf(struct{ G unexportedFields }{}),
		}, {
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "H",
						Type: pipepb.FieldType_builder{
							Nullable: true,
							LogicalType: pipepb.LogicalType_builder{
								Urn: "schema.unexportedFields",
								Representation: pipepb.FieldType_builder{
									RowType: pipepb.RowType_builder{
										Schema: pipepb.Schema_builder{
											Fields: []*pipepb.Field{
												pipepb.Field_builder{
													Name: "D",
													Type: pipepb.FieldType_builder{
														LogicalType: pipepb.LogicalType_builder{
															Urn: "uint64",
															Representation: pipepb.FieldType_builder{
																AtomicType: pipepb.AtomicType_INT64.Enum(),
															}.Build(),
														}.Build(),
													}.Build(),
												}.Build(),
											},
										}.Build(),
									}.Build(),
								}.Build(),
							}.Build(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			rt: reflect.TypeOf(struct{ H *unexportedFields }{}),
		}, {
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "E",
						Type: pipepb.FieldType_builder{
							AtomicType: pipepb.AtomicType_INT16.Enum(),
						}.Build(),
					}.Build(),
				},
				Options: []*pipepb.Option{optGoNillable(), logicalOption("*schema.exportedFunc")},
			}.Build(),
			rt: exportedFuncType,
		}, {
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "Q",
						Type: pipepb.FieldType_builder{
							LogicalType: pipepb.LogicalType_builder{
								Urn: "schema.myInt",
								Representation: pipepb.FieldType_builder{
									LogicalType: pipepb.LogicalType_builder{
										Urn: "int",
										Representation: pipepb.FieldType_builder{
											AtomicType: pipepb.AtomicType_INT64.Enum(),
										}.Build(),
									}.Build(),
								}.Build(),
							}.Build(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			rt: anotherType,
		}, {
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name:    "Exported",
						Options: []*pipepb.Option{optGoEmbedded()},
						Type: pipepb.FieldType_builder{
							RowType: pipepb.RowType_builder{
								Schema: pipepb.Schema_builder{
									Fields: []*pipepb.Field{
										pipepb.Field_builder{
											Name: "G",
											Type: pipepb.FieldType_builder{
												LogicalType: pipepb.LogicalType_builder{
													Urn: "schema.myInt",
													Representation: pipepb.FieldType_builder{
														LogicalType: pipepb.LogicalType_builder{
															Urn: "int",
															Representation: pipepb.FieldType_builder{
																AtomicType: pipepb.AtomicType_INT64.Enum(),
															}.Build(),
														}.Build(),
													}.Build(),
												}.Build(),
											}.Build(),
										}.Build(),
									},
								}.Build(),
							}.Build(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			rt: hasEmbeddedType,
		}, {
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name:    "Exported",
						Options: []*pipepb.Option{optGoEmbedded()},
						Type: pipepb.FieldType_builder{
							Nullable: true,
							RowType: pipepb.RowType_builder{
								Schema: pipepb.Schema_builder{
									Fields: []*pipepb.Field{
										pipepb.Field_builder{
											Name: "G",
											Type: pipepb.FieldType_builder{
												LogicalType: pipepb.LogicalType_builder{
													Urn: "schema.myInt",
													Representation: pipepb.FieldType_builder{
														LogicalType: pipepb.LogicalType_builder{
															Urn: "int",
															Representation: pipepb.FieldType_builder{
																AtomicType: pipepb.AtomicType_INT64.Enum(),
															}.Build(),
														}.Build(),
													}.Build(),
												}.Build(),
											}.Build(),
										}.Build(),
									},
								}.Build(),
							}.Build(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			rt: hasEmbeddedPtrType,
		}, {
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "T",
						Type: pipepb.FieldType_builder{
							AtomicType: pipepb.AtomicType_STRING.Enum(),
						}.Build(),
					}.Build(),
				},
				Options: []*pipepb.Option{optGoNillable()},
			}.Build(),
			rt: reflect.TypeOf(&struct {
				myInt
				T string
				i int
			}{}),
		}, {
			st: pipepb.Schema_builder{
				Options: []*pipepb.Option{
					logicalOption("schema.exportedFunc"),
				},
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "V",
						Type: pipepb.FieldType_builder{
							AtomicType: pipepb.AtomicType_INT16.Enum(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			rt: reflect.TypeOf(exportedFunc{}),
		}, {
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "U",
						Type: pipepb.FieldType_builder{
							LogicalType: pipepb.LogicalType_builder{
								Urn: "schema.exportedFunc",
								Representation: pipepb.FieldType_builder{
									RowType: pipepb.RowType_builder{
										Schema: pipepb.Schema_builder{
											Fields: []*pipepb.Field{
												pipepb.Field_builder{
													Name: "V",
													Type: pipepb.FieldType_builder{
														AtomicType: pipepb.AtomicType_INT16.Enum(),
													}.Build(),
												}.Build(),
											},
										}.Build(),
									}.Build(),
								}.Build(),
							}.Build(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			rt: reflect.TypeOf(struct {
				U exportedFunc
			}{}),
		}, {
			st: pipepb.Schema_builder{
				Fields: []*pipepb.Field{
					pipepb.Field_builder{
						Name: "U",
						Type: pipepb.FieldType_builder{
							LogicalType: pipepb.LogicalType_builder{
								Urn: "schema.nonRegisteredLogical",
								Representation: pipepb.FieldType_builder{
									RowType: pipepb.RowType_builder{
										Schema: pipepb.Schema_builder{
											Fields: []*pipepb.Field{
												pipepb.Field_builder{
													Name: "K",
													Type: pipepb.FieldType_builder{
														AtomicType: pipepb.AtomicType_INT32.Enum(),
													}.Build(),
												}.Build(),
											},
										}.Build(),
									}.Build(),
								}.Build(),
							}.Build(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			rt: reflect.TypeOf(struct {
				U nonRegisteredLogical
			}{}),
		},
	}

	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("%v", test.rt), func(t *testing.T) {
			reg := NewRegistry()
			preRegLogicalTypes(reg)
			reg.RegisterLogicalType(ToLogicalType(exportedFuncType.Elem().String(), exportedFuncType.Elem(), reflect.TypeOf(struct{ V int16 }{})))
			reg.RegisterLogicalType(ToLogicalType(nonRegisteredLogicalType.String(), nonRegisteredLogicalType, reflect.TypeOf(struct{ K int32 }{})))
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
