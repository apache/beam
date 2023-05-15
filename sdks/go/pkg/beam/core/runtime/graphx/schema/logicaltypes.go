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
	"sync"

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

	// Maps logical type identifiers to their reflect.Type and the schema representation.
	// the type identifier is the reflect.Type name, and included in the proto as well.
	// We don't treat all types as "logical" types.
	// ... why don't we treat all types as Logical types?
	logicalTypes           map[string]LogicalType
	logicalTypeIdentifiers map[reflect.Type]string

	// toReconcile contains a list of types that have been registered
	// but not yet processed. Registration actually happens on first
	// call to ToType or FromType or once Initialize is called on beam.Init.
	toReconcile []reflect.Type
}

// NewRegistry creates an initialized LogicalTypeRegistry.
func NewRegistry() *Registry {
	return &Registry{
		typeToSchema:    map[reflect.Type]*pipepb.Schema{},
		idToType:        map[string]reflect.Type{},
		syntheticToUser: map[reflect.Type]reflect.Type{},

		logicalTypes:           map[string]LogicalType{},
		logicalTypeIdentifiers: map[reflect.Type]string{},
		logicalTypeProviders:   map[reflect.Type]LogicalTypeProvider{},
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
	// TODO add duplication checks.
	r.logicalTypeIdentifiers[lt.GoType()] = lt.ID()
	r.logicalTypes[lt.ID()] = lt
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
}
