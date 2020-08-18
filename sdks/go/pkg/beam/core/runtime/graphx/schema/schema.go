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

// Package schema contains utility functions for relating Go types and Beam Schemas.
//
// Not all Go types can be converted to schemas. This is Go is more expressive than
// Beam schemas. Just as not all Go types can be serialized, similarly,
// not all Beam Schemas will have a conversion to Go types, until the correct
// mechanism exists in the SDK to handle them.
//
// While efforts will be made to have conversions be reversable, this will not
// be possible in all instances. Eg. Go arrays as fields will be converted to
// Beam Arrays, but a Beam Array type will map by default to a Go slice.
package schema

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
)

var lastShortID int64

// TODO(BEAM-9615): Replace with UUIDs.
func getNextID() string {
	id := atomic.AddInt64(&lastShortID, 1)
	// No reason not to use the smallest string short ids possible.
	return strconv.FormatInt(id, 36)
}

var (
	// Maps types to schemas for reuse, caching the UUIDs.
	typeToSchema = map[reflect.Type]*pipepb.Schema{}
	// Maps synthetic types to user types. Keys must be generated from a schema.
	// This works around using the generated type assertion shims failing to type assert.
	// Type assertion isn't assignability, which is closer to how the reflection based
	// shims operate.
	// User types are mapped to themselves to also signify they've been registered.
	syntheticToUser = map[reflect.Type]reflect.Type{}
)

// Registered returns whether the given type has been registered with
// the schema package.
func Registered(ut reflect.Type) bool {
	_, ok := syntheticToUser[ut]
	return ok
}

// RegisterType converts the type to it's schema representation, and converts it back to
// a synthetic type so we can map from the synthetic type back to the user type.
// Recursively registers other named struct types in any component parts.
func RegisterType(ut reflect.Type) {
	registerType(ut, map[reflect.Type]struct{}{})
}

func registerType(ut reflect.Type, seen map[reflect.Type]struct{}) {
	if _, ok := syntheticToUser[ut]; ok {
		return
	}
	if _, ok := seen[ut]; ok {
		return // already processed in this pass, don't reprocess.
	}
	seen[ut] = struct{}{}

	// Lets do some recursion to register fundamental type parts.
	t := ut
	switch t.Kind() {
	case reflect.Map:
		registerType(t.Key(), seen)
		fallthrough
	case reflect.Array, reflect.Slice, reflect.Ptr:
		registerType(t.Elem(), seen)
		return
	case reflect.Struct: // What we expect here.
	default:
		return
	}
	runtime.RegisterType(ut)

	for i := 0; i < t.NumField(); i++ {
		sf := ut.Field(i)
		registerType(sf.Type, seen)
	}

	schm, err := FromType(ut)
	if err != nil {
		panic(errors.WithContextf(err, "converting %v to schema", ut))
	}
	synth, err := ToType(schm)
	if err != nil {
		panic(errors.WithContextf(err, "converting %v's back to a synthetic type", ut))
	}
	synth = reflectx.SkipPtr(synth)
	ut = reflectx.SkipPtr(ut)
	syntheticToUser[synth] = ut
	syntheticToUser[reflect.PtrTo(synth)] = reflect.PtrTo(ut)
	syntheticToUser[ut] = ut
	syntheticToUser[reflect.PtrTo(ut)] = reflect.PtrTo(ut)
}

// FromType returns a Beam Schema of the passed in type.
// Returns an error if the type cannot be converted to a Schema.
func FromType(ot reflect.Type) (*pipepb.Schema, error) {
	if reflectx.SkipPtr(ot).Kind() != reflect.Struct {
		return nil, errors.Errorf("cannot convert %v to schema. FromType only converts structs to schemas", ot)
	}
	schm, err := structToSchema(ot)
	if err != nil {
		return nil, err
	}
	if ot.Kind() == reflect.Ptr {
		schm.Options = append(schm.Options, &pipepb.Option{
			Name: optGoNillable,
		})
	}
	return schm, nil
}

// Schema Option urns.
const (
	// optGoNillable indicates that this top level schema should be returned as a pointer type.
	optGoNillable = "beam:schema:go:nillable:v1"
	// optGoInt indicates that this field should be decoded to an int, rather than an int64.
	optGoInt = "beam:schema:go:int:v1"
	// Since maps, arrays, and iterables don't have options, we need additional options
	// to handle plain go integers.
	optGoIntKey  = "beam:schema:go:intkey:v1"  // For int map keys
	optGoIntElem = "beam:schema:go:intelem:v1" // For int values for maps,slices, and arrays
)

func goIntOptions(t reflect.Type) []*pipepb.Option {
	var opts []*pipepb.Option
	switch t.Kind() {
	case reflect.Int:
		opts = append(opts, &pipepb.Option{
			Name: optGoInt,
		})
	case reflect.Map:
		if t.Key().Kind() == reflect.Int {
			opts = append(opts, &pipepb.Option{
				Name: optGoIntKey,
			})
		}
		fallthrough
	case reflect.Array, reflect.Slice:
		if t.Elem().Kind() == reflect.Int {
			opts = append(opts, &pipepb.Option{
				Name: optGoIntElem,
			})
		}
	}
	return opts
}

// nillableFromOptions converts the passed in type to it's pointer version
// if the option is present. This permits go types to be pointers.
func nillableFromOptions(opts []*pipepb.Option, t reflect.Type) reflect.Type {
	return checkOptions(opts, optGoNillable, reflect.PtrTo(t))
}

func checkOptions(opts []*pipepb.Option, urn string, rt reflect.Type) reflect.Type {
	for _, opt := range opts {
		if opt.GetName() == urn {
			return rt
		}
	}
	return nil
}

func structToSchema(ot reflect.Type) (*pipepb.Schema, error) {
	if schm, ok := typeToSchema[ot]; ok {
		return schm, nil
	}
	t := reflectx.SkipPtr(ot)
	fields := make([]*pipepb.Field, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f, err := structFieldToField(t.Field(i))
		if err != nil {
			return nil, errors.Wrapf(err, "cannot convert field %v to schema", t.Field(i).Name)
		}
		fields = append(fields, f)
	}

	schm := &pipepb.Schema{
		Fields: fields,
		Id:     getNextID(),
	}
	typeToSchema[ot] = schm
	return schm, nil
}

func structFieldToField(sf reflect.StructField) (*pipepb.Field, error) {
	name := sf.Name
	if tag := sf.Tag.Get("beam"); tag != "" {
		name, _ = parseTag(tag)
	}
	ftype, opts, err := reflectTypeToFieldType(sf.Type)
	if err != nil {
		return nil, err
	}
	return &pipepb.Field{
		Name:    name,
		Type:    ftype,
		Options: opts,
	}, nil
}

func reflectTypeToFieldType(ot reflect.Type) (*pipepb.FieldType, []*pipepb.Option, error) {
	var isPtr bool
	t := ot
	if t.Kind() == reflect.Ptr {
		isPtr = true
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Map:
		kt, _, err := reflectTypeToFieldType(t.Key())
		if err != nil {
			return nil, nil, errors.Wrapf(err, "unable to convert key of %v to schema field", ot)
		}
		vt, _, err := reflectTypeToFieldType(t.Elem())
		if err != nil {
			return nil, nil, errors.Wrapf(err, "unable to convert value of %v to schema field", ot)
		}
		return &pipepb.FieldType{
			Nullable: isPtr,
			TypeInfo: &pipepb.FieldType_MapType{
				MapType: &pipepb.MapType{
					KeyType:   kt,
					ValueType: vt,
				},
			},
		}, goIntOptions(t), nil
	case reflect.Struct:
		sch, err := structToSchema(t)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "unable to convert %v to schema field", ot)
		}
		return &pipepb.FieldType{
			Nullable: isPtr,
			TypeInfo: &pipepb.FieldType_RowType{
				RowType: &pipepb.RowType{
					Schema: sch,
				},
			},
		}, nil, nil
	case reflect.Slice, reflect.Array:
		// Special handling for []byte
		if t == reflectx.ByteSlice {
			return &pipepb.FieldType{
				Nullable: isPtr,
				TypeInfo: &pipepb.FieldType_AtomicType{
					AtomicType: pipepb.AtomicType_BYTES,
				},
			}, nil, nil
		}
		vt, _, err := reflectTypeToFieldType(t.Elem())
		if err != nil {
			return nil, nil, errors.Wrapf(err, "unable to convert element type of %v to schema field", ot)
		}
		opts := goIntOptions(t)
		return &pipepb.FieldType{
			Nullable: isPtr,
			TypeInfo: &pipepb.FieldType_ArrayType{
				ArrayType: &pipepb.ArrayType{
					ElementType: vt,
				},
			},
		}, opts, nil
	case reflect.Interface, reflect.Chan, reflect.UnsafePointer, reflect.Complex128, reflect.Complex64:
		return nil, nil, errors.Errorf("unable to convert unsupported type %v to schema", ot)
	default: // must be an atomic type
		if enum, ok := reflectTypeToAtomicTypeMap[t.Kind()]; ok {
			return &pipepb.FieldType{
				Nullable: isPtr,
				TypeInfo: &pipepb.FieldType_AtomicType{
					AtomicType: enum,
				},
			}, goIntOptions(t), nil
		}
		return nil, nil, errors.Errorf("unable to map %v to pipepb.AtomicType", t)
	}
}

var reflectTypeToAtomicTypeMap = map[reflect.Kind]pipepb.AtomicType{
	reflect.Uint8:   pipepb.AtomicType_BYTE,
	reflect.Int16:   pipepb.AtomicType_INT16,
	reflect.Int32:   pipepb.AtomicType_INT32,
	reflect.Int64:   pipepb.AtomicType_INT64,
	reflect.Int:     pipepb.AtomicType_INT64,
	reflect.Float32: pipepb.AtomicType_FLOAT,
	reflect.Float64: pipepb.AtomicType_DOUBLE,
	reflect.String:  pipepb.AtomicType_STRING,
	reflect.Bool:    pipepb.AtomicType_BOOLEAN,
}

// ToType returns a Go type of the passed in Schema.
// Types returned by ToType are always of Struct kind.
// Returns an error if the Schema cannot be converted to a type.
func ToType(s *pipepb.Schema) (reflect.Type, error) {
	fields := make([]reflect.StructField, 0, len(s.GetFields()))
	for _, sf := range s.GetFields() {
		rf, err := fieldToStructField(sf)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot convert schema field %v to field", sf.GetName())
		}
		fields = append(fields, rf)
	}
	ret := reflect.StructOf(fields)
	if ut, ok := syntheticToUser[ret]; ok {
		ret = ut
	}
	if t := nillableFromOptions(s.GetOptions(), ret); t != nil {
		return t, nil
	}
	return ret, nil
}

func fieldToStructField(sf *pipepb.Field) (reflect.StructField, error) {
	name := sf.GetName()
	rt, err := fieldTypeToReflectType(sf.GetType(), sf.Options)
	if err != nil {
		return reflect.StructField{}, err
	}

	rsf := reflect.StructField{
		Name: strings.ToUpper(name[:1]) + name[1:], // Go field name must be capitalized for export and encoding.
		Type: rt,
	}
	// Add a name tag if they don't match.
	if name != rsf.Name {
		rsf.Tag = reflect.StructTag(fmt.Sprintf("beam:\"%s\"", name))
	}
	return rsf, nil
}

var atomicTypeToReflectType = map[pipepb.AtomicType]reflect.Type{
	pipepb.AtomicType_BYTE:    reflectx.Uint8,
	pipepb.AtomicType_INT16:   reflectx.Int16,
	pipepb.AtomicType_INT32:   reflectx.Int32,
	pipepb.AtomicType_INT64:   reflectx.Int64,
	pipepb.AtomicType_FLOAT:   reflectx.Float32,
	pipepb.AtomicType_DOUBLE:  reflectx.Float64,
	pipepb.AtomicType_STRING:  reflectx.String,
	pipepb.AtomicType_BOOLEAN: reflectx.Bool,
	pipepb.AtomicType_BYTES:   reflectx.ByteSlice,
}

func fieldTypeToReflectType(sft *pipepb.FieldType, opts []*pipepb.Option) (reflect.Type, error) {
	var t reflect.Type
	switch sft.GetTypeInfo().(type) {
	case *pipepb.FieldType_AtomicType:
		var ok bool
		if t, ok = atomicTypeToReflectType[sft.GetAtomicType()]; !ok {
			return nil, errors.Errorf("unknown atomic type: %v", sft.GetAtomicType())
		}
		// Handle duplicate type matchings.
		if optT := checkOptions(opts, optGoInt, reflectx.Int); optT != nil {
			t = optT
		}
	case *pipepb.FieldType_ArrayType:
		rt, err := fieldTypeToReflectType(sft.GetArrayType().GetElementType(), nil)
		if err != nil {
			return nil, errors.Wrap(err, "unable to convert array element type")
		}
		// Handle duplicate type matchings.
		if optT := checkOptions(opts, optGoIntElem, reflectx.Int); optT != nil {
			rt = optT
		}
		t = reflect.SliceOf(rt)
	case *pipepb.FieldType_MapType:
		kt, err := fieldTypeToReflectType(sft.GetMapType().GetKeyType(), nil)
		if err != nil {
			return nil, errors.Wrap(err, "unable to convert map key type")
		}
		if optT := checkOptions(opts, optGoIntKey, reflectx.Int); optT != nil {
			kt = optT
		}
		vt, err := fieldTypeToReflectType(sft.GetMapType().GetValueType(), nil)
		if err != nil {
			return nil, errors.Wrap(err, "unable to convert map value type")
		}
		if optT := checkOptions(opts, optGoIntElem, reflectx.Int); optT != nil {
			vt = optT
		}
		t = reflect.MapOf(kt, vt) // Panics for invalid map keys (slices/iterables)
	case *pipepb.FieldType_RowType:
		rt, err := ToType(sft.GetRowType().GetSchema())
		if err != nil {
			return nil, errors.Wrapf(err, "unable to convert row type: %v", sft.GetRowType().GetSchema().GetId())
		}
		t = rt
	// case *pipepb.FieldType_IterableType:
	// TODO(BEAM-9615): handle IterableTypes.

	//case *pipepb.FieldType_LogicalType:
	// TODO(BEAM-9615): handle LogicalTypes types.
	//sft.GetLogicalType().

	// Logical Types are for things that have more specialized user representation already, or
	// things like Time or protocol buffers.
	// They would be encoded with the schema encoding.

	default:
		return nil, errors.Errorf("unknown fieldtype: %T", sft.GetTypeInfo())
	}
	if sft.GetNullable() {
		return reflect.PtrTo(t), nil
	}
	return t, nil
}

// parseTag splits a struct field's beam tag into its name and
// comma-separated options.
func parseTag(tag string) (string, options) {
	if idx := strings.Index(tag, ","); idx != -1 {
		return tag[:idx], options(tag[idx+1:])
	}
	return tag, options("")
}

type options string

// TODO(BEAM-9615): implement looking up specific options from the tags.
