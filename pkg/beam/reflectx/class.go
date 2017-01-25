package reflectx

import (
	"fmt"
	"reflect"
)

// Class is the type "class" of data as distinguished by the runtime. The class
// determines what kind of coding and/or conversion to be inserted.
type Class int

const (
	// Invalid type, such as Function, which is not valid as data.
	Invalid Class = iota
	// Concrete type, such as int, string, struct { .. }.
	Concrete
	// Universal type: reflect.Value. (Maybe interface{}, too?)
	Universal
	// Encoded type: []byte. A coder is necessary to move to and from an
	// encoded type. Certain transformations can be written purely as
	// operations on encoded types and thus be generic and efficient.
	Encoded
	// composite type: KV, GBK. It may be partially encoded or universal.
	Composite
)

func (c Class) IsGeneric() bool {
	return c == Universal || c == Encoded
}

// IsAssignable returns true iff a from value can be assigned to the to value,
// possibly through a coder. Does not apply to KV, GBK types.
func IsAssignable(from, to reflect.Type) bool {
	a := ClassOf(from)
	b := ClassOf(to)

	if a == Invalid || a == Composite || b == Invalid || b == Composite {
		return false
	}
	if a == Concrete && b == Concrete {
		return from.AssignableTo(to)
	}
	return true
}

// ClassOf returns the class of a given type. The class is Invalid, if the
// type is not suitable as data.
func ClassOf(t reflect.Type) Class {
	switch {
	case t == ByteSlice:
		return Encoded
	case t == ReflectValue:
		return Universal
	case IsComposite(t):
		return Composite
	case isConcrete(t):
		return Concrete
	default:
		return Invalid
	}
}

// isConcrete returns true iff the given type is a valid "concrete" data type. Such
// data must be fully serializable. Functions and channels are examples of invalid
// types. Note the Encoded types are also concrete, if not in a top-level context.
func isConcrete(t reflect.Type) bool {
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
		return isConcrete(t.Elem())

	case reflect.Ptr:
		return false // TBD

	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if !isConcrete(t.Field(i).Type) {
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

func IsComposite(t reflect.Type) bool {
	_, _, ok := UnfoldComposite(t)
	return ok
}

func UnfoldComposite(t reflect.Type) (reflect.Type, reflect.Type, bool) {
	key, ok := FindTaggedField(t, KeyTag)
	if !ok {
		return nil, nil, false
	}
	value, ok := FindTaggedField(t, ValueTag, ValuesTag)
	if !ok {
		return nil, nil, false
	}
	return key.Type, value.Type, true
}

func MakeComposite(model, key, value reflect.Type) (reflect.Type, error) {
	switch {
	case IsKV(model):
		return MakeKV(key, value)
	case IsGBK(model):
		return MakeGBK(key, value)
	default:
		return nil, fmt.Errorf("Model not composite type: %v", model)
	}
}
