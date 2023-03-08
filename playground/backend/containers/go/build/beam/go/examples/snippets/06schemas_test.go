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

package snippets

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder/testutil"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func atomicSchemaField(name string, typ pipepb.AtomicType) *pipepb.Field {
	return &pipepb.Field{
		Name: name,
		Type: &pipepb.FieldType{
			TypeInfo: &pipepb.FieldType_AtomicType{
				AtomicType: typ,
			},
		},
	}
}

func rowSchemaField(name string, typ *pipepb.Schema) *pipepb.Field {
	return &pipepb.Field{
		Name: name,
		Type: &pipepb.FieldType{
			TypeInfo: &pipepb.FieldType_RowType{
				RowType: &pipepb.RowType{
					Schema: typ,
				},
			},
		},
	}
}

func listSchemaField(name string, typ *pipepb.Field) *pipepb.Field {
	return &pipepb.Field{
		Name: name,
		Type: &pipepb.FieldType{
			TypeInfo: &pipepb.FieldType_ArrayType{
				ArrayType: &pipepb.ArrayType{
					ElementType: typ.GetType(),
				},
			},
		},
	}
}

func nillable(f *pipepb.Field) *pipepb.Field {
	f.Type.Nullable = true
	return f
}

func TestSchemaTypes(t *testing.T) {
	transactionSchema := &pipepb.Schema{
		Fields: []*pipepb.Field{
			atomicSchemaField("bank", pipepb.AtomicType_STRING),
			atomicSchemaField("purchaseAmount", pipepb.AtomicType_DOUBLE),
		},
	}
	shippingAddressSchema := &pipepb.Schema{
		Fields: []*pipepb.Field{
			atomicSchemaField("streetAddress", pipepb.AtomicType_STRING),
			atomicSchemaField("city", pipepb.AtomicType_STRING),
			nillable(atomicSchemaField("state", pipepb.AtomicType_STRING)),
			atomicSchemaField("country", pipepb.AtomicType_STRING),
			atomicSchemaField("postCode", pipepb.AtomicType_STRING),
		},
	}

	tests := []struct {
		rt     reflect.Type
		st     *pipepb.Schema
		preReg func(reg *schema.Registry)
	}{{
		rt: reflect.TypeOf(Transaction{}),
		st: transactionSchema,
	}, {
		rt: reflect.TypeOf(ShippingAddress{}),
		st: shippingAddressSchema,
	}, {
		rt: reflect.TypeOf(Purchase{}),
		st: &pipepb.Schema{
			Fields: []*pipepb.Field{
				atomicSchemaField("userId", pipepb.AtomicType_STRING),
				atomicSchemaField("itemId", pipepb.AtomicType_INT64),
				rowSchemaField("shippingAddress", shippingAddressSchema),
				atomicSchemaField("cost", pipepb.AtomicType_INT64),
				listSchemaField("transactions",
					rowSchemaField("n/a", transactionSchema)),
			},
		},
	}, {
		rt: tnType,
		st: &pipepb.Schema{
			Fields: []*pipepb.Field{
				atomicSchemaField("seconds", pipepb.AtomicType_INT64),
				atomicSchemaField("nanos", pipepb.AtomicType_INT32),
			},
		},
		preReg: func(reg *schema.Registry) {
			reg.RegisterLogicalType(schema.ToLogicalType(tnType.Name(), tnType, tnStorageType))
		},
	}}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.rt), func(t *testing.T) {
			reg := schema.NewRegistry()
			if test.preReg != nil {
				test.preReg(reg)
			}
			{
				got, err := reg.FromType(test.rt)
				if err != nil {
					t.Fatalf("error FromType(%v) = %v", test.rt, err)
				}
				if d := cmp.Diff(test.st, got,
					protocmp.Transform(),
					protocmp.IgnoreFields(proto.Message(&pipepb.Schema{}), "id", "options"),
				); d != "" {
					t.Errorf("diff (-want, +got): %v", d)
				}
			}
		})
	}
}

func TestSchema_validate(t *testing.T) {
	tests := []struct {
		rt               reflect.Type
		p                beam.SchemaProvider
		logical, storage any
	}{
		{
			rt:      tnType,
			p:       &TimestampNanosProvider{},
			logical: TimestampNanos(time.Unix(2300003, 456789)),
			storage: tnStorage{},
		},
	}
	for _, test := range tests {
		sc := &testutil.SchemaCoder{
			CmpOptions: cmp.Options{
				cmp.Comparer(func(a, b TimestampNanos) bool {
					return a.Seconds() == b.Seconds() && a.Nanos() == b.Nanos()
				})},
		}
		sc.Validate(t, test.rt, test.p.BuildEncoder, test.p.BuildDecoder, test.storage, test.logical)
	}
}
