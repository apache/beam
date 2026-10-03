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
	"reflect"
	"testing"

	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
)

func TestAtomicValueProtoRoundTrip(t *testing.T) {
	tests := []struct {
		value any
		want  pipepb.AtomicType
	}{
		{true, pipepb.AtomicType_BOOLEAN},
		{uint8(200), pipepb.AtomicType_BYTE},
		{int16(-300), pipepb.AtomicType_INT16},
		{int32(70000), pipepb.AtomicType_INT32},
		{int64(-1 << 40), pipepb.AtomicType_INT64},
		{float32(1.5), pipepb.AtomicType_FLOAT},
		{float64(2.25), pipepb.AtomicType_DOUBLE},
		{"text", pipepb.AtomicType_STRING},
		{[]byte("bytes"), pipepb.AtomicType_BYTES},
	}
	for _, test := range tests {
		t.Run(reflect.TypeOf(test.value).String(), func(t *testing.T) {
			ft, fv, err := atomicValueToProto(reflect.ValueOf(test.value))
			if err != nil {
				t.Fatalf("atomicValueToProto(%v) = %v, want nil error", test.value, err)
			}
			if got := ft.GetAtomicType(); got != test.want {
				t.Errorf("atomicValueToProto(%v) field type = %v, want %v", test.value, got, test.want)
			}
			got, err := atomicValueFromProto(ft, fv)
			if err != nil {
				t.Fatalf("atomicValueFromProto(%v, %v) = %v, want nil error", ft, fv, err)
			}
			if d := cmp.Diff(test.value, got.Interface()); d != "" {
				t.Errorf("round trip of %v: diff (-want, +got): %v", test.value, d)
			}
		})
	}
}

func TestAtomicValueToProto_Unsupported(t *testing.T) {
	type named int32
	for _, value := range []any{int(1), uint32(1), named(1), struct{ A int32 }{}, []int32{1}} {
		if _, _, err := atomicValueToProto(reflect.ValueOf(value)); err == nil {
			t.Errorf("atomicValueToProto(%T) = nil error, want error", value)
		}
	}
}

func TestAtomicValueFromProto_Mismatch(t *testing.T) {
	stringType := &pipepb.FieldType{
		TypeInfo: &pipepb.FieldType_AtomicType{AtomicType: pipepb.AtomicType_STRING},
	}
	int32Value := &pipepb.FieldValue{
		FieldValue: &pipepb.FieldValue_AtomicValue{
			AtomicValue: &pipepb.AtomicTypeValue{Value: &pipepb.AtomicTypeValue_Int32{Int32: 3}},
		},
	}
	if _, err := atomicValueFromProto(stringType, int32Value); err == nil {
		t.Errorf("atomicValueFromProto(STRING, int32 value) = nil error, want error")
	}
	rowType := &pipepb.FieldType{
		TypeInfo: &pipepb.FieldType_RowType{RowType: &pipepb.RowType{Schema: &pipepb.Schema{}}},
	}
	if _, err := atomicValueFromProto(rowType, int32Value); err == nil {
		t.Errorf("atomicValueFromProto(ROW, int32 value) = nil error, want error")
	}
	if _, err := atomicValueFromProto(stringType, &pipepb.FieldValue{}); err == nil {
		t.Errorf("atomicValueFromProto(STRING, null value) = nil error, want error")
	}
}

func TestAtomicValueToProto_ByteIsSigned(t *testing.T) {
	_, fv, err := atomicValueToProto(reflect.ValueOf(uint8(200)))
	if err != nil {
		t.Fatalf("atomicValueToProto(uint8(200)) = %v, want nil error", err)
	}
	if got := fv.GetAtomicValue().GetByte(); got != -56 {
		t.Errorf("atomicValueToProto(uint8(200)) byte = %v, want -56", got)
	}
	byteType := &pipepb.FieldType{
		TypeInfo: &pipepb.FieldType_AtomicType{AtomicType: pipepb.AtomicType_BYTE},
	}
	got, err := atomicValueFromProto(byteType, fv)
	if err != nil {
		t.Fatalf("atomicValueFromProto(BYTE, -56) = %v, want nil error", err)
	}
	if got.Interface() != uint8(200) {
		t.Errorf("atomicValueFromProto(BYTE, -56) = %v, want uint8(200)", got)
	}
}
