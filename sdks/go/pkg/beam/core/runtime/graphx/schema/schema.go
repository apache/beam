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

// FromType returns a Beam Schema of the passed in type.
// Returns an error if the type cannot be converted to a Schema.
func FromType(ot reflect.Type) (*pipepb.Schema, error) {
	return defaultRegistry.FromType(ot)
}

// ToType returns a Go type of the passed in Schema.
// Types returned by ToType are always of Struct kind.
// Returns an error if the Schema cannot be converted to a type.
func ToType(s *pipepb.Schema) (reflect.Type, error) {
	return defaultRegistry.ToType(s)
}

// Registered returns whether the given type has been registered with
// the default schema registry.
func Registered(ut reflect.Type) bool {
	return defaultRegistry.Registered(ut)
}

// RegisterType converts the type to it's schema representation, and converts it back to
// a synthetic type so we can map from the synthetic type back to the user type.
// Recursively registers other named struct types in any component parts.
func RegisterType(ut reflect.Type) {
	defaultRegistry.RegisterType(ut)
}

var lastShortID int64

// TODO(BEAM-9615): Replace with UUIDs.
func (r *Registry) getNextID() string {
	id := atomic.AddInt64(&r.lastShortID, 1)
	// No reason not to use the smallest string short ids possible.
	return strconv.FormatInt(id, 36)
}

// Registered returns whether the given type has been registered with
// the schema package.
func (r *Registry) Registered(ut reflect.Type) bool {
	_, ok := r.syntheticToUser[ut]
	return ok
}

// RegisterType converts the type to it's schema representation, and converts it back to
// a synthetic type so we can map from the synthetic type back to the user type.
// Recursively registers other named struct types in any component parts.
func (r *Registry) RegisterType(ut reflect.Type) {
	r.registerType(ut, map[reflect.Type]struct{}{})
}

func (r *Registry) registerType(ut reflect.Type, seen map[reflect.Type]struct{}) {
	if _, ok := r.syntheticToUser[ut]; ok {
		return
	}
	if _, ok := seen[ut]; ok {
		return // already processed in this pass, don't reprocess.
	}
	seen[ut] = struct{}{}

	// Lets do some recursion to register fundamental type parts.
	t := ut

	if lID, ok := r.logicalTypeIdentifiers[t]; ok {
		lt := r.logicalTypes[lID]
		r.addToMaps(lt.StorageType(), t)
		return
	}
	for _, lti := range r.logicalTypeInterfaces {
		if !t.Implements(lti) {
			continue
		}
		p := r.logicalTypeProviders[lti]
		st, err := p(t)
		if err != nil {
			panic(errors.Wrapf(err, "unable to convert LogicalType[%v] using provider for %v", t, lti))
		}
		if st == nil {
			continue
		}
		r.RegisterLogicalType(ToLogicalType(t.String(), t, st))
		r.addToMaps(st, t)
		return
	}

	switch t.Kind() {
	case reflect.Map:
		r.registerType(t.Key(), seen)
		fallthrough
	case reflect.Array, reflect.Slice, reflect.Ptr:
		r.registerType(t.Elem(), seen)
		return
	case reflect.Struct: // What we expect here.
	default:
		return
	}
	runtime.RegisterType(ut)

	for i := 0; i < t.NumField(); i++ {
		sf := ut.Field(i)
		r.registerType(sf.Type, seen)
	}

	schm, err := r.FromType(ut)
	if err != nil {
		panic(errors.WithContextf(err, "converting %v to schema", ut))
	}
	synth, err := r.ToType(schm)
	if err != nil {
		panic(errors.WithContextf(err, "converting %v's back to a synthetic type", ut))
	}
	r.addToMaps(synth, ut)
}

func (r *Registry) addToMaps(synth, ut reflect.Type) {
	synth = reflectx.SkipPtr(synth)
	ut = reflectx.SkipPtr(ut)
	r.syntheticToUser[synth] = ut
	r.syntheticToUser[reflect.PtrTo(synth)] = reflect.PtrTo(ut)
	r.syntheticToUser[ut] = ut
	r.syntheticToUser[reflect.PtrTo(ut)] = reflect.PtrTo(ut)
}

// FromType returns a Beam Schema of the passed in type.
// Returns an error if the type cannot be converted to a Schema.
func (r *Registry) FromType(ot reflect.Type) (*pipepb.Schema, error) {
	if reflectx.SkipPtr(ot).Kind() != reflect.Struct {
		return nil, errors.Errorf("cannot convert %v to schema. FromType only converts structs to schemas", ot)
	}
	schm, err := r.structToSchema(ot)
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
)

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

func (r *Registry) structToSchema(ot reflect.Type) (*pipepb.Schema, error) {
	if schm, ok := r.typeToSchema[ot]; ok {
		return schm, nil
	}

	t := reflectx.SkipPtr(ot)
	// Check if a logical type was registered that matches this struct type directly
	// and if so, extract the schema from it for use.
	if lID, ok := r.logicalTypeIdentifiers[ot]; ok {
		lt := r.logicalTypes[lID]
		ftype, err := r.reflectTypeToFieldType(lt.StorageType())
		if err != nil {
			return nil, errors.Wrapf(err, "unable to convert LogicalType[%v]'s storage type %v for Go type of %v to a schema", lID, lt.StorageType(), lt.GoType())
		}
		schm := ftype.GetRowType().GetSchema()
		r.typeToSchema[ot] = schm
		return schm, nil
	}

	fields := make([]*pipepb.Field, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		isUnexported := sf.PkgPath != ""
		if isUnexported {
			continue // ignore unexported fields here.
		}
		f, err := r.structFieldToField(sf)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot convert field %v to schema", t.Field(i).Name)
		}
		fields = append(fields, f)
	}

	schm := &pipepb.Schema{
		Fields: fields,
		Id:     r.getNextID(),
	}
	r.typeToSchema[ot] = schm
	return schm, nil
}

func (r *Registry) structFieldToField(sf reflect.StructField) (*pipepb.Field, error) {
	name := sf.Name
	if tag := sf.Tag.Get("beam"); tag != "" {
		name, _ = parseTag(tag)
	}
	ftype, err := r.reflectTypeToFieldType(sf.Type)
	if err != nil {
		return nil, err
	}
	return &pipepb.Field{
		Name: name,
		Type: ftype,
	}, nil
}

func (r *Registry) reflectTypeToFieldType(ot reflect.Type) (*pipepb.FieldType, error) {
	if lID, ok := r.logicalTypeIdentifiers[ot]; ok {
		lt := r.logicalTypes[lID]
		ftype, err := r.reflectTypeToFieldType(lt.StorageType())
		if err != nil {
			return nil, errors.Wrapf(err, "unable to convert LogicalType[%v]'s storage type %v for Go type of %v to a schema field", lID, lt.StorageType(), lt.GoType())
		}
		return &pipepb.FieldType{
			TypeInfo: &pipepb.FieldType_LogicalType{
				LogicalType: &pipepb.LogicalType{
					Urn:            lID,
					Representation: ftype,
					// TODO(BEAM-9615): Handle type Arguments.
				},
			},
		}, nil
	}
	for _, lti := range r.logicalTypeInterfaces {
		if !ot.Implements(lti) {
			continue
		}
		p := r.logicalTypeProviders[lti]
		st, err := p(ot)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to convert LogicalType[%v] using provider for %v schema field", ot, lti)
		}
		if st == nil {
			continue
		}
		ftype, err := r.reflectTypeToFieldType(st)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to convert LogicalType[%v]'s storage type %v for Go type of %v to a schema field", lti, st, ot)
		}
		return &pipepb.FieldType{
			TypeInfo: &pipepb.FieldType_LogicalType{
				LogicalType: &pipepb.LogicalType{
					Urn:            ot.String(),
					Representation: ftype,
					// TODO(BEAM-9615): Handle type Arguments.
				},
			},
		}, nil
	}

	var isPtr bool
	t := ot
	if t.Kind() == reflect.Ptr {
		isPtr = true
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Map:
		kt, err := r.reflectTypeToFieldType(t.Key())
		if err != nil {
			return nil, errors.Wrapf(err, "unable to convert key of %v to schema field", ot)
		}
		vt, err := r.reflectTypeToFieldType(t.Elem())
		if err != nil {
			return nil, errors.Wrapf(err, "unable to convert value of %v to schema field", ot)
		}
		return &pipepb.FieldType{
			Nullable: isPtr,
			TypeInfo: &pipepb.FieldType_MapType{
				MapType: &pipepb.MapType{
					KeyType:   kt,
					ValueType: vt,
				},
			},
		}, nil
	case reflect.Struct:
		sch, err := r.structToSchema(t)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to convert %v to schema field", ot)
		}
		return &pipepb.FieldType{
			Nullable: isPtr,
			TypeInfo: &pipepb.FieldType_RowType{
				RowType: &pipepb.RowType{
					Schema: sch,
				},
			},
		}, nil
	case reflect.Slice, reflect.Array:
		// Special handling for []byte
		if t == reflectx.ByteSlice {
			return &pipepb.FieldType{
				Nullable: isPtr,
				TypeInfo: &pipepb.FieldType_AtomicType{
					AtomicType: pipepb.AtomicType_BYTES,
				},
			}, nil
		}
		vt, err := r.reflectTypeToFieldType(t.Elem())
		if err != nil {
			return nil, errors.Wrapf(err, "unable to convert element type of %v to schema field", ot)
		}
		return &pipepb.FieldType{
			Nullable: isPtr,
			TypeInfo: &pipepb.FieldType_ArrayType{
				ArrayType: &pipepb.ArrayType{
					ElementType: vt,
				},
			},
		}, nil
	case reflect.Interface, reflect.Chan, reflect.UnsafePointer, reflect.Complex128, reflect.Complex64:
		return nil, errors.Errorf("unable to convert unsupported type %v to schema", ot)
	default: // must be an atomic type
		if enum, ok := reflectTypeToAtomicTypeMap[t.Kind()]; ok {
			return &pipepb.FieldType{
				Nullable: isPtr,
				TypeInfo: &pipepb.FieldType_AtomicType{
					AtomicType: enum,
				},
			}, nil
		}
		return nil, errors.Errorf("unable to map %v to pipepb.AtomicType", t)
	}
}

var reflectTypeToAtomicTypeMap = map[reflect.Kind]pipepb.AtomicType{
	reflect.Uint8:   pipepb.AtomicType_BYTE,
	reflect.Int16:   pipepb.AtomicType_INT16,
	reflect.Int32:   pipepb.AtomicType_INT32,
	reflect.Int64:   pipepb.AtomicType_INT64,
	reflect.Float32: pipepb.AtomicType_FLOAT,
	reflect.Float64: pipepb.AtomicType_DOUBLE,
	reflect.String:  pipepb.AtomicType_STRING,
	reflect.Bool:    pipepb.AtomicType_BOOLEAN,
}

// ToType returns a Go type of the passed in Schema.
// Types returned by ToType are always of Struct kind.
// Returns an error if the Schema cannot be converted to a type.
func (r *Registry) ToType(s *pipepb.Schema) (reflect.Type, error) {
	fields := make([]reflect.StructField, 0, len(s.GetFields()))
	for _, sf := range s.GetFields() {
		rf, err := r.fieldToStructField(sf)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot convert schema field %v to field", sf.GetName())
		}
		fields = append(fields, rf)
	}
	ret := reflect.StructOf(fields)
	if ut, ok := r.syntheticToUser[ret]; ok {
		ret = ut
	}
	if t := nillableFromOptions(s.GetOptions(), ret); t != nil {
		return t, nil
	}
	return ret, nil
}

func (r *Registry) fieldToStructField(sf *pipepb.Field) (reflect.StructField, error) {
	name := sf.GetName()
	rt, err := r.fieldTypeToReflectType(sf.GetType(), sf.Options)
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

func (r *Registry) fieldTypeToReflectType(sft *pipepb.FieldType, opts []*pipepb.Option) (reflect.Type, error) {
	var t reflect.Type
	switch sft.GetTypeInfo().(type) {
	case *pipepb.FieldType_AtomicType:
		var ok bool
		if t, ok = atomicTypeToReflectType[sft.GetAtomicType()]; !ok {
			return nil, errors.Errorf("unknown atomic type: %v", sft.GetAtomicType())
		}
	case *pipepb.FieldType_ArrayType:
		rt, err := r.fieldTypeToReflectType(sft.GetArrayType().GetElementType(), nil)
		if err != nil {
			return nil, errors.Wrap(err, "unable to convert array element type")
		}
		t = reflect.SliceOf(rt)
	case *pipepb.FieldType_MapType:
		kt, err := r.fieldTypeToReflectType(sft.GetMapType().GetKeyType(), nil)
		if err != nil {
			return nil, errors.Wrap(err, "unable to convert map key type")
		}
		vt, err := r.fieldTypeToReflectType(sft.GetMapType().GetValueType(), nil)
		if err != nil {
			return nil, errors.Wrap(err, "unable to convert map value type")
		}
		t = reflect.MapOf(kt, vt) // Panics for invalid map keys (slices/iterables)
	case *pipepb.FieldType_RowType:
		rt, err := r.ToType(sft.GetRowType().GetSchema())
		if err != nil {
			return nil, errors.Wrapf(err, "unable to convert row type: %v", sft.GetRowType().GetSchema().GetId())
		}
		t = rt
	// case *pipepb.FieldType_IterableType:
	// TODO(BEAM-9615): handle IterableTypes (eg. CoGBK values)

	case *pipepb.FieldType_LogicalType:
		lst := sft.GetLogicalType()
		identifier := lst.GetUrn()
		lt, ok := r.logicalTypes[identifier]
		if !ok {
			return nil, errors.Errorf("unknown logical type: %v", identifier)
		}
		t = lt.GoType()

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
