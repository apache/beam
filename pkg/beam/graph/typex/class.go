package typex

import (
	"fmt"
	"reflect"
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
	// Universal type: T, U, V, W, X, Y, Z.
	Universal
	// Composite type: KV, GBK, CoGBk, WindowedValue.
	Composite
)

// TODO(herohde) 5/16/2017: maybe we should add more classes, so that every
// reasonable type (such as error) is not Invalid, even though it it not
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
	case IsConcrete(t):
		return Concrete
	default:
		return Invalid
	}
}

// IsConcrete returns true iff the given type is a valid "concrete" data type. Such
// data must be fully serializable. Functions and channels are examples of invalid
// types.
func IsConcrete(t reflect.Type) bool {
	if t == nil {
		return false
	}

	switch t.Kind() {
	case reflect.Invalid, reflect.UnsafePointer, reflect.Uintptr, reflect.Interface:
		return false // no unmanageable types

	case reflect.Chan, reflect.Func:
		return false // no unserializable types

	case reflect.Map, reflect.Array:
		return false // TBD

	case reflect.Complex64, reflect.Complex128:
		return false // TBD

	case reflect.Slice:
		return IsConcrete(t.Elem())

	case reflect.Ptr:
		return false // TBD

	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if !IsConcrete(t.Field(i).Type) {
				return false
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

	case reflect.String:
		return true

	default:
		panic(fmt.Sprintf("Unexpected type kind: %v", t))
	}
}

func IsUniversal(t reflect.Type) bool {
	switch t {
	case TType, UType, VType, WType, XType, YType, ZType:
		return true
	default:
		return false
	}
}

func IsComposite(t reflect.Type) bool {
	switch t {
	case KVType, GBKType, CoGBKType, WindowedValueType:
		return true
	default:
		return false
	}
}
