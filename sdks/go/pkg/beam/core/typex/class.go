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

package typex

import (
	"fmt"
	"reflect"
	"unicode"
	"unicode/utf8"

	"github.com/golang/protobuf/proto"
)

// Class is the type "class" of data as distinguished by the runtime. The class
// determines what kind of coding and/or conversion to be inserted. Special types
// such EventTime, error and reflect.Type are Invalid in this context.
type Class int

const (
	// Invalid type, such as Function, which is not valid as data.
	Invalid Class = iota
	// Concrete type, such as int, string, struct { .. }.
	Concrete
	// Universal type: T, U, V, W, X, Y, Z. They act as type variables
	// for type checking purposes, but -- unlike type variables -- are
	// not instantiated at runtime.
	Universal
	// Container type: []. A Go-generic container type that both can be
	// represented as a reflect.Type and may incur runtime conversions.
	// The component cannot be a Composite.
	Container
	// Composite type: KV, CoGBk, WindowedValue. Beam-generic types
	// that cannot be represented as a single reflect.Type.
	Composite
)

var protoMessageType = reflect.TypeOf((*proto.Message)(nil)).Elem()

// TODO(herohde) 5/16/2017: maybe we should add more classes, so that every
// reasonable type (such as error) is not Invalid, even though it is not
// valid in FullType. "Special", say? Right now, a valid DoFn signature may
// have "Invalid" parameter types, which might be confusing. Or maybe rename
// as DataClass to make the narrower scope clearer?

// ClassOf returns the class of a given type. The class is Invalid, if the
// type is not suitable as data.
func ClassOf(t reflect.Type) Class {
	switch {
	case IsUniversal(t):
		return Universal
	case IsComposite(t):
		return Composite
	case IsContainer(t): // overrules IsConcrete
		return Container
	case IsConcrete(t):
		return Concrete
	default:
		return Invalid
	}
}

// IsConcrete returns true iff the given type is a valid "concrete" data type. Such
// data must be fully serializable. Functions and channels are examples of invalid
// types. Aggregate types with no universals are considered concrete here.
func IsConcrete(t reflect.Type) bool {
	if t == nil || t == EventTimeType {
		return false
	}

	// TODO(BEAM-3306): the coder registry should be consulted here for user
	// specified types and their coders.
	if t.Implements(protoMessageType) {
		return true
	}

	switch t.Kind() {
	case reflect.Invalid, reflect.UnsafePointer, reflect.Uintptr, reflect.Interface:
		return false // no unmanageable types

	case reflect.Chan, reflect.Func:
		return false // no unserializable types

	case reflect.Map, reflect.Array:
		return false // TBD

	case reflect.Slice:
		return IsConcrete(t.Elem())

	case reflect.Ptr:
		return false // TBD

	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			// We ignore private fields under the assumption that they are
			// either not needed or will be coded manually. For combiner
			// accumulators, we need types that need non-trivial coding. Also,
			// Go serialization schemes in general ignore private fields.

			f := t.Field(i)
			if len(f.Name) > 0 {
				r, _ := utf8.DecodeRuneInString(f.Name)
				if unicode.IsUpper(r) && !IsConcrete(f.Type) {
					return false
				}
			}
		}
		return true

	case reflect.Bool:
		return true

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true

	case reflect.Float32, reflect.Float64:
		return true

	case reflect.Complex64, reflect.Complex128:
		return true

	case reflect.String:
		return true

	default:
		panic(fmt.Sprintf("Unexpected type kind: %v", t))
	}
}

// IsContainer returns true iff the given type is an container data type,
// such as []int or []T.
func IsContainer(t reflect.Type) bool {
	switch {
	case IsList(t):
		if IsUniversal(t.Elem()) || IsConcrete(t.Elem()) {
			return true
		}
		return IsContainer(t.Elem())
	default:
		return false
	}
}

// IsList returns true iff the given type is a slice.
func IsList(t reflect.Type) bool {
	return t.Kind() == reflect.Slice
}

// IsUniversal returns true iff the given type is one of the predefined
// universal types: T, U, V, W, X, Y or Z.
func IsUniversal(t reflect.Type) bool {
	switch t {
	case TType, UType, VType, WType, XType, YType, ZType:
		return true
	default:
		return false
	}
}

// IsComposite returns true iff the given type is one of the predefined
// Composite marker types: KV, CoGBK or WindowedValue.
func IsComposite(t reflect.Type) bool {
	switch t {
	case KVType, CoGBKType, WindowedValueType:
		return true
	default:
		return false
	}
}
