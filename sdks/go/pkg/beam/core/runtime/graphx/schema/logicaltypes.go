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

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

var (
	// Maps logical type identifiers to their reflect.Type and the schema representation.
	// the type identifier is the reflect.Type name, and included in the proto as well.
	// We don't treat all types as "logical" types.
	// ... why don't we treat all types as Logical types?
	logicalTypes       = map[string]LogicalType{}
	logicalIdentifiers = map[reflect.Type]string{}
)

// LogicalType is an interface between custom Go types, and schema storage types.
//
// A LogicalType is a way to define a new type that can be stored in a schema field
// using a known underlying type for storage. The storage type must be comprised of
// known schema field types, or pre-registered LogicalTypes. LogicalTypes may not be
// mutually recursive at any level of indirection.
//End
type LogicalType interface {
	ID() string
	ArgumentType() reflect.Type
	ArgumentValue() reflect.Value
	GoType() reflect.Type
	StorageType() reflect.Type
	// ToStorageType converts an instance of the Go type to the schema storage type.
	ToStorageType(input reflect.Value) reflect.Value
	// ToGoType converts an instance of the given schema storage type to the Go type.
	ToGoType(base reflect.Value) reflect.Value
}

// RegisterLogicalType registers a logical type with the beam schema system.
// A logical type is a type that has distinct representations and storage.
//
// RegisterLogicalType will panic if the storage type of the LogicalType
// instance is not a valid storage type.
func RegisterLogicalType(lt LogicalType) {
	// Validates that the storage type has known handling.
	st := lt.StorageType()
	_, err := reflectTypeToFieldType(st)
	if err != nil {
		panic(fmt.Sprintf("LogicalType[%v] has an invalid StorageType %v: %v", lt.ID(), st, err))
	}
	logicalIdentifiers[lt.GoType()] = lt.ID()
	logicalTypes[lt.ID()] = lt
}

// convertibleLogicalType uses reflect.Value.Convert to change the Go
// type to the schema storage type and back again. Does not support
// type arguments.
//
// gotT and storageT must be convertible to each other.
type convertibleLogicalType struct {
	identifier    string
	goT, storageT reflect.Type
}

func (l *convertibleLogicalType) ID() string {
	return l.identifier
}

func (l *convertibleLogicalType) ArgumentType() reflect.Type {
	return nil
}

func (l *convertibleLogicalType) ArgumentValue() reflect.Value {
	return reflect.Value{}
}

func (l *convertibleLogicalType) GoType() reflect.Type {
	return l.goT
}

func (l *convertibleLogicalType) StorageType() reflect.Type {
	return l.storageT
}

func (l *convertibleLogicalType) ToStorageType(value reflect.Value) reflect.Value {
	return value.Convert(l.storageT)
}

func (l *convertibleLogicalType) ToGoType(storage reflect.Value) reflect.Value {
	return storage.Convert(l.goT)
}

// NewConvertibleLogicalType creates a LogicalType where the go type and storage representation
// can be converted between each other with reflect.Value.Convert.
func NewConvertibleLogicalType(identifier string, goType, storageType reflect.Type) LogicalType {
	if !(goType.ConvertibleTo(storageType) && storageType.ConvertibleTo(goType)) {
		panic(fmt.Sprintf("Can't create ConvertibleTo LogicalType: %v and %v are not convertable to each other", goType, storageType))
	}
	return &convertibleLogicalType{identifier: identifier, goT: goType, storageT: storageType}
}

func init() {
	RegisterLogicalType(NewConvertibleLogicalType("int", reflectx.Int, reflectx.Int64))
	RegisterLogicalType(NewConvertibleLogicalType("uint32", reflectx.Uint32, reflectx.Int32))
	RegisterLogicalType(NewConvertibleLogicalType("uint64", reflectx.Uint64, reflectx.Int64))
	RegisterLogicalType(NewConvertibleLogicalType("uint16", reflectx.Uint16, reflectx.Int16))
	RegisterLogicalType(NewConvertibleLogicalType("int8", reflectx.Int8, reflectx.Uint8))
}
