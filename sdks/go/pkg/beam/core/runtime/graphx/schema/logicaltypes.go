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
	"io"
	"reflect"
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

var (
	defaultRegistry = NewRegistry()
)

// RegisterLogicalType registers a logical type with the beam schema system.
// A logical type is a type that has distinct representations and storage.
//
// RegisterLogicalType will panic if the storage type of the LogicalType
// instance is not a valid storage type.
func RegisterLogicalType(lt LogicalType) {
	defaultRegistry.RegisterLogicalType(lt)
}

// RegisterLogicalTypeProvider allows registration of providers for interface types.
func RegisterLogicalTypeProvider(rt reflect.Type, ltp LogicalTypeProvider) {
	defaultRegistry.RegisterLogicalTypeProvider(rt, ltp)
}

// LogicalTypeProvider produces a logical type for a given Go type.
//
// If unable to produce a logical type, it instead produces an error.
// Typically used to handle mapping LogicalTypes from interface types
// to a concrete implementation. The provider will be passed a
// type, and will produce an appropriate LogicalType for it.
type LogicalTypeProvider = func(reflect.Type) (reflect.Type, error)

// Registry retains mappings from go types to Schemas and LogicalTypes.
type Registry struct {
	rwmu sync.RWMutex

	typeToSchema    map[reflect.Type]*pipepb.Schema
	idToType        map[string]reflect.Type
	syntheticToUser map[reflect.Type]reflect.Type

	logicalTypeProviders  map[reflect.Type]LogicalTypeProvider
	logicalTypeInterfaces []reflect.Type

	// logicalTypes maps registered logical types by identifier and argument.
	// The identifier is included in the proto as the logical type URN.
	// We don't treat all types as "logical" types.
	logicalTypes map[logicalTypeKey]LogicalType
	// logicalTypeByGoType maps Go types to their registered logical type.
	logicalTypeByGoType map[reflect.Type]LogicalType

	// toReconcile contains a list of types that have been registered
	// but not yet processed. Registration actually happens on first
	// call to ToType or FromType or once Initialize is called on beam.Init.
	toReconcile []reflect.Type
}

// logicalTypeKey identifies a registered logical type. argument is nil for
// logical types without an argument, and the argument value otherwise.
type logicalTypeKey struct {
	identifier string
	argument   any
}

// NewRegistry creates an initialized LogicalTypeRegistry.
func NewRegistry() *Registry {
	return &Registry{
		typeToSchema:    map[reflect.Type]*pipepb.Schema{},
		idToType:        map[string]reflect.Type{},
		syntheticToUser: map[reflect.Type]reflect.Type{},

		logicalTypes:         map[logicalTypeKey]LogicalType{},
		logicalTypeByGoType:  map[reflect.Type]LogicalType{},
		logicalTypeProviders: map[reflect.Type]LogicalTypeProvider{},
	}
}

// RegisterLogicalType a single logical type.
func (r *Registry) RegisterLogicalType(lt LogicalType) {
	// Validates that the storage type has known handling.
	st := lt.StorageType()
	_, err := r.reflectTypeToFieldType(st)
	if err != nil {
		panic(fmt.Sprintf("LogicalType[%v] has an invalid StorageType %v: %v", lt.ID(), st, err))
	}
	if len(lt.ID()) == 0 {
		panic(fmt.Sprintf("invalid logical type, bad id: %v -> %v", lt.GoType(), lt.StorageType()))
	}
	if lt.argT != nil {
		if _, _, err := atomicValueToProto(lt.argV); err != nil {
			panic(fmt.Sprintf("LogicalType[%v] has an invalid argument %v: %v", lt.ID(), lt.argV, err))
		}
		if !lt.argV.Comparable() || !lt.argV.Equal(lt.argV) {
			panic(fmt.Sprintf("LogicalType[%v] has the argument %v which does not compare equal to itself", lt.ID(), lt.argV))
		}
	}
	// TODO add duplication checks.
	r.logicalTypeByGoType[lt.GoType()] = lt
	r.logicalTypes[lt.key()] = lt
}

// lookupLogicalType returns the logical type registered for identifier and,
// when argument is valid, that argument. A logical type registered without an
// argument matches any argument.
func (r *Registry) lookupLogicalType(identifier string, argument reflect.Value) (LogicalType, bool) {
	if argument.IsValid() && argument.Comparable() {
		if lt, ok := r.logicalTypes[logicalTypeKey{identifier: identifier, argument: argument.Interface()}]; ok {
			return lt, true
		}
	}
	lt, ok := r.logicalTypes[logicalTypeKey{identifier: identifier}]
	return lt, ok
}

// RegisterLogicalTypeProvider allows registration of providers for interface types.
func (r *Registry) RegisterLogicalTypeProvider(rt reflect.Type, ltp LogicalTypeProvider) {
	if rt.Kind() != reflect.Interface {
		panic(fmt.Sprintf("Logical Types must be registered with interface types. %v is not an interface type.", rt))
	}
	if rt.NumMethod() == 0 {
		panic(fmt.Sprintf("Logical Types may not be registered with empty interface types. %v has no methods.", rt))
	}
	r.logicalTypeProviders[rt] = ltp
	r.logicalTypeInterfaces = append(r.logicalTypeInterfaces, rt)
}

// LogicalType is a mapping between custom Go types, and their schema equivalent storage types.
//
// A LogicalType is a way to define a type that can be stored in a schema field
// using a known underlying type for storage. The storage type must be comprised of
// known schema field types, or pre-registered LogicalTypes.
//
// LogicalTypes may not be mutually recursive at any level of indirection.
// LogicalTypes must map from a Go type to a single Schema Equivalent storage type.
type LogicalType struct {
	identifier          string
	goT, storageT, argT reflect.Type
	argV                reflect.Value
}

// ID is a unique identifier for the logical type.
func (l *LogicalType) ID() string {
	return l.identifier
}

// ArgumentType returns the Go type of the argument for parameterized types.
func (l *LogicalType) ArgumentType() reflect.Type {
	return l.argT
}

// ArgumentValue returns the Go value of the argument for parameterized types.
func (l *LogicalType) ArgumentValue() reflect.Value {
	return l.argV
}

// GoType returns the Go type of the logical type. This is the type in a Go
// field.
func (l *LogicalType) GoType() reflect.Type {
	return l.goT
}

// StorageType is the schema equivalent representation of this logical type.
// The storage type is how the logical type is encoded in bytes, and if the
// logical type is unknown, can be decoded into a value of this type instead.
func (l *LogicalType) StorageType() reflect.Type {
	return l.storageT
}

// ToLogicalType creates a LogicalType, indicating that there's a conversion from one to the other.
func ToLogicalType(identifier string, goType, storageType reflect.Type) LogicalType {
	return LogicalType{identifier: identifier, goT: goType, storageT: storageType}
}

// ToLogicalTypeWithArgument creates a LogicalType with an argument, for
// parameterized logical types such as a fixed length string and its length.
// The argument is included in the schema representation of the logical type,
// and must be a value of a built in Go type with an atomic schema type other
// than []byte. It is matched by equality, so a floating point NaN is not a
// valid argument.
func ToLogicalTypeWithArgument(identifier string, goType, storageType reflect.Type, argument any) LogicalType {
	lt := ToLogicalType(identifier, goType, storageType)
	if argument != nil {
		lt.argT = reflect.TypeOf(argument)
		lt.argV = reflect.ValueOf(argument)
	}
	return lt
}

// key returns the registry key of the logical type.
func (l *LogicalType) key() logicalTypeKey {
	if l.argT == nil {
		return logicalTypeKey{identifier: l.identifier}
	}
	return logicalTypeKey{identifier: l.identifier, argument: l.argV.Interface()}
}

func preRegLogicalTypes(r *Registry) {
	r.RegisterLogicalType(ToLogicalType("int", reflectx.Int, reflectx.Int64))
	r.RegisterLogicalType(ToLogicalType("int8", reflectx.Int8, reflectx.Int64))
	r.RegisterLogicalType(ToLogicalType("uint16", reflectx.Uint16, reflectx.Int16))
	r.RegisterLogicalType(ToLogicalType("uint32", reflectx.Uint32, reflectx.Int32))
	r.RegisterLogicalType(ToLogicalType("uint64", reflectx.Uint64, reflectx.Int64))
	r.RegisterLogicalType(ToLogicalType("uint", reflectx.Uint, reflectx.Int64))
}

func init() {
	preRegLogicalTypes(defaultRegistry)
	registerStandardLogicalTypes(defaultRegistry)
}

// RegisterLogicalTypeConversion registers a portable logical type identified
// by urn. Values of GoT are stored in schema rows as values of StorageT,
// converted with toStorage and fromStorage, and encoded with the row field
// encoding of StorageT. StorageT must have a schema representation: an atomic
// type, a struct of such types, or a registered logical type. A struct kind
// GoT with a non row StorageT can be used as a field of a row, but not as a
// top level schema type.
//
// RegisterLogicalTypeConversion must be called before beam.Init(), and
// conventionally is called in a package init() function.
func RegisterLogicalTypeConversion[GoT, StorageT any](urn string, toStorage func(GoT) (StorageT, error), fromStorage func(StorageT) (GoT, error)) {
	registerLogicalTypeConversion(defaultRegistry, ToLogicalType(urn, typeOf[GoT](), typeOf[StorageT]()), toStorage, fromStorage)
}

// RegisterParameterizedLogicalTypeConversion is RegisterLogicalTypeConversion
// for a logical type with an argument, such as a timestamp and its precision.
// Each argument value is registered with its own Go type, and a schema field
// with the urn and that argument maps to GoT. See ToLogicalTypeWithArgument
// for the supported argument types.
func RegisterParameterizedLogicalTypeConversion[GoT, StorageT any](urn string, argument any, toStorage func(GoT) (StorageT, error), fromStorage func(StorageT) (GoT, error)) {
	registerLogicalTypeConversion(defaultRegistry, ToLogicalTypeWithArgument(urn, typeOf[GoT](), typeOf[StorageT](), argument), toStorage, fromStorage)
}

// RegisterLogicalTypeCoder registers a portable logical type identified by
// urn whose wire encoding differs from the row field encoding of StorageT.
// StorageT is only used as the schema representation of the logical type.
// enc and dec produce and consume the encoded bytes of a value directly, and
// must match the encoding of the other SDKs for the urn.
//
// RegisterLogicalTypeCoder must be called before beam.Init(), and
// conventionally is called in a package init() function.
func RegisterLogicalTypeCoder[GoT, StorageT any](urn string, enc func(GoT, io.Writer) error, dec func(io.Reader) (GoT, error)) {
	registerLogicalTypeCoder(defaultRegistry, ToLogicalType(urn, typeOf[GoT](), typeOf[StorageT]()), enc, dec)
}

func typeOf[T any]() reflect.Type {
	return reflect.TypeOf((*T)(nil)).Elem()
}

// registerLogicalTypeConversion registers lt with the registry and registers
// row coder providers for its Go type, built from the row field coder of the
// storage type and the conversion functions.
func registerLogicalTypeConversion[GoT, StorageT any](r *Registry, lt LogicalType, toStorage func(GoT) (StorageT, error), fromStorage func(StorageT) (GoT, error)) {
	st := lt.StorageType()
	r.RegisterLogicalType(lt)
	coder.RegisterSchemaProviders(lt.GoType(),
		func(reflect.Type) (func(any, io.Writer) error, error) {
			enc, err := coder.RowFieldEncoderForType(st)
			if err != nil {
				return nil, err
			}
			return func(v any, w io.Writer) error {
				s, err := toStorage(v.(GoT))
				if err != nil {
					return err
				}
				return enc(s, w)
			}, nil
		},
		func(reflect.Type) (func(io.Reader) (any, error), error) {
			dec, err := coder.RowFieldDecoderForType(st)
			if err != nil {
				return nil, err
			}
			return func(rd io.Reader) (any, error) {
				s, err := dec(rd)
				if err != nil {
					return nil, err
				}
				return fromStorage(s.(StorageT))
			}, nil
		})
}

// registerLogicalTypeCoder registers lt with the registry and registers enc
// and dec as the row coder providers for its Go type.
func registerLogicalTypeCoder[GoT any](r *Registry, lt LogicalType, enc func(GoT, io.Writer) error, dec func(io.Reader) (GoT, error)) {
	r.RegisterLogicalType(lt)
	coder.RegisterSchemaProviders(lt.GoType(),
		func(reflect.Type) (func(any, io.Writer) error, error) {
			return func(v any, w io.Writer) error {
				return enc(v.(GoT), w)
			}, nil
		},
		func(reflect.Type) (func(io.Reader) (any, error), error) {
			return func(rd io.Reader) (any, error) {
				return dec(rd)
			}, nil
		})
}

// RegisterPassThroughLogicalType registers a logical type identified by urn
// whose values are used as their representation type. A schema field with
// the urn maps to goType and uses the row encoding of goType, and any
// argument of the logical type is ignored. Go values of goType are not mapped
// back to the logical type, so a struct with a goType field produces a schema
// field of the representation type. This suits the standard fixed and
// variable length string and bytes logical types, whose argument only
// constrains the length.
//
// RegisterPassThroughLogicalType must be called before beam.Init(), and
// conventionally is called in a package init() function.
func RegisterPassThroughLogicalType(urn string, goType reflect.Type) {
	defaultRegistry.RegisterPassThroughLogicalType(urn, goType)
}

// RegisterPassThroughLogicalType registers a logical type whose values are
// used as their representation type. See the package level function.
func (r *Registry) RegisterPassThroughLogicalType(urn string, goType reflect.Type) {
	if len(urn) == 0 {
		panic(fmt.Sprintf("invalid pass through logical type, bad urn for %v", goType))
	}
	if _, err := r.reflectTypeToFieldType(goType); err != nil {
		panic(fmt.Sprintf("pass through LogicalType[%v] has an invalid type %v: %v", urn, goType, err))
	}
	lt := ToLogicalType(urn, goType, goType)
	r.logicalTypes[lt.key()] = lt
}
