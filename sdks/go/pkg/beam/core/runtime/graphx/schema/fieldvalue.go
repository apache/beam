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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

// atomicFieldType returns the schema FieldType of an atomic Go type.
// The Go type must be one of the built in types that map directly to an
// AtomicType: bool, uint8, int16, int32, int64, float32, float64, string
// and []byte.
func atomicFieldType(t reflect.Type) (*pipepb.FieldType, error) {
	var at pipepb.AtomicType
	if t == reflectx.ByteSlice {
		at = pipepb.AtomicType_BYTES
	} else if enum, ok := reflectTypeToAtomicTypeMap[t.Kind()]; ok && reflectKindToTypeMap[t.Kind()] == t {
		at = enum
	} else {
		return nil, errors.Errorf("%v is not an atomic schema type", t)
	}
	return &pipepb.FieldType{
		TypeInfo: &pipepb.FieldType_AtomicType{
			AtomicType: at,
		},
	}, nil
}

// atomicValueToProto converts a Go value of an atomic type to its schema
// FieldType and FieldValue. The BYTE schema type is signed, so a uint8 is
// converted through int8.
func atomicValueToProto(rv reflect.Value) (*pipepb.FieldType, *pipepb.FieldValue, error) {
	if !rv.IsValid() {
		return nil, nil, errors.New("invalid value")
	}
	ft, err := atomicFieldType(rv.Type())
	if err != nil {
		return nil, nil, err
	}
	var av *pipepb.AtomicTypeValue
	switch ft.GetAtomicType() {
	case pipepb.AtomicType_BYTE:
		av = &pipepb.AtomicTypeValue{Value: &pipepb.AtomicTypeValue_Byte{Byte: int32(int8(rv.Uint()))}}
	case pipepb.AtomicType_INT16:
		av = &pipepb.AtomicTypeValue{Value: &pipepb.AtomicTypeValue_Int16{Int16: int32(rv.Int())}}
	case pipepb.AtomicType_INT32:
		av = &pipepb.AtomicTypeValue{Value: &pipepb.AtomicTypeValue_Int32{Int32: int32(rv.Int())}}
	case pipepb.AtomicType_INT64:
		av = &pipepb.AtomicTypeValue{Value: &pipepb.AtomicTypeValue_Int64{Int64: rv.Int()}}
	case pipepb.AtomicType_FLOAT:
		av = &pipepb.AtomicTypeValue{Value: &pipepb.AtomicTypeValue_Float{Float: float32(rv.Float())}}
	case pipepb.AtomicType_DOUBLE:
		av = &pipepb.AtomicTypeValue{Value: &pipepb.AtomicTypeValue_Double{Double: rv.Float()}}
	case pipepb.AtomicType_STRING:
		av = &pipepb.AtomicTypeValue{Value: &pipepb.AtomicTypeValue_String_{String_: rv.String()}}
	case pipepb.AtomicType_BOOLEAN:
		av = &pipepb.AtomicTypeValue{Value: &pipepb.AtomicTypeValue_Boolean{Boolean: rv.Bool()}}
	case pipepb.AtomicType_BYTES:
		av = &pipepb.AtomicTypeValue{Value: &pipepb.AtomicTypeValue_Bytes{Bytes: rv.Bytes()}}
	default:
		return nil, nil, errors.Errorf("unhandled atomic type %v", ft.GetAtomicType())
	}
	return ft, &pipepb.FieldValue{FieldValue: &pipepb.FieldValue_AtomicValue{AtomicValue: av}}, nil
}

// atomicValueFromProto converts a schema FieldValue of the atomic FieldType ft
// to a Go value of the matching built in type.
func atomicValueFromProto(ft *pipepb.FieldType, fv *pipepb.FieldValue) (reflect.Value, error) {
	at, ok := ft.GetTypeInfo().(*pipepb.FieldType_AtomicType)
	if !ok {
		return reflect.Value{}, errors.Errorf("field type %v is not atomic", ft)
	}
	t, ok := atomicTypeToReflectType[at.AtomicType]
	if !ok {
		return reflect.Value{}, errors.Errorf("unknown atomic type: %v", at.AtomicType)
	}
	av := fv.GetAtomicValue()
	if av == nil {
		return reflect.Value{}, errors.Errorf("field value %v is not atomic", fv)
	}
	if got := atomicValueType(av); got != at.AtomicType {
		return reflect.Value{}, errors.Errorf("atomic value %v does not match field type %v", av, at.AtomicType)
	}
	rv := reflect.New(t).Elem()
	switch v := av.GetValue().(type) {
	case *pipepb.AtomicTypeValue_Byte:
		rv.SetUint(uint64(uint8(v.Byte)))
	case *pipepb.AtomicTypeValue_Int16:
		rv.SetInt(int64(v.Int16))
	case *pipepb.AtomicTypeValue_Int32:
		rv.SetInt(int64(v.Int32))
	case *pipepb.AtomicTypeValue_Int64:
		rv.SetInt(v.Int64)
	case *pipepb.AtomicTypeValue_Float:
		rv.SetFloat(float64(v.Float))
	case *pipepb.AtomicTypeValue_Double:
		rv.SetFloat(v.Double)
	case *pipepb.AtomicTypeValue_String_:
		rv.SetString(v.String_)
	case *pipepb.AtomicTypeValue_Boolean:
		rv.SetBool(v.Boolean)
	case *pipepb.AtomicTypeValue_Bytes:
		rv.SetBytes(v.Bytes)
	}
	return rv, nil
}

// atomicValueType returns the atomic type of the value set in av, or
// UNSPECIFIED when no value is set.
func atomicValueType(av *pipepb.AtomicTypeValue) pipepb.AtomicType {
	switch av.GetValue().(type) {
	case *pipepb.AtomicTypeValue_Byte:
		return pipepb.AtomicType_BYTE
	case *pipepb.AtomicTypeValue_Int16:
		return pipepb.AtomicType_INT16
	case *pipepb.AtomicTypeValue_Int32:
		return pipepb.AtomicType_INT32
	case *pipepb.AtomicTypeValue_Int64:
		return pipepb.AtomicType_INT64
	case *pipepb.AtomicTypeValue_Float:
		return pipepb.AtomicType_FLOAT
	case *pipepb.AtomicTypeValue_Double:
		return pipepb.AtomicType_DOUBLE
	case *pipepb.AtomicTypeValue_String_:
		return pipepb.AtomicType_STRING
	case *pipepb.AtomicTypeValue_Boolean:
		return pipepb.AtomicType_BOOLEAN
	case *pipepb.AtomicTypeValue_Bytes:
		return pipepb.AtomicType_BYTES
	}
	return pipepb.AtomicType_UNSPECIFIED
}
