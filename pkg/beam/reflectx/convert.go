package reflectx

import (
	"reflect"
)

// TODO: we need nested pack/unpack to reuse them, i.e., generate a converter.

// Convert converts a value to the target type, notably converting differences
// in field position and typing for composites. The types must be structurally
// identical.
func Convert(value reflect.Value, to reflect.Type) reflect.Value {
	from := value.Type()

	switch {
	case from == to:
		return value

	case from == T:
		// We need to drop T to obtain the underlying type of the value.

		var untyped interface{}
		untyped = value.Interface()
		return Convert(reflect.ValueOf(untyped), to)

	case IsComposite(from) && IsComposite(to):
		toK, toV, _ := UnfoldComposite(to)

		keyFn, valueFn := UnpackFn(from)
		packFn := PackFn(to)

		return packFn(Convert(keyFn(value), toK), Convert(valueFn(value), toV))

	default:
		return value
	}
}

// PackFn returns a Composite constructor. It avoids some reflection for each
// element.
func PackFn(t reflect.Type) func(reflect.Value, reflect.Value) reflect.Value {
	key, ok := FindTaggedField(t, KeyTag)
	if !ok {
		panic("Key tag not found")
	}
	value, ok := FindTaggedField(t, ValueTag, ValuesTag)
	if !ok {
		panic("Value tag not found")
	}

	return func(k reflect.Value, v reflect.Value) reflect.Value {
		ret := reflect.New(t).Elem()
		ret.FieldByIndex(key.Index).Set(k)
		ret.FieldByIndex(value.Index).Set(v)
		return ret
	}
}

// UnpackFn returns a Composite deconstructor. It avoids some reflection for each
// element.
func UnpackFn(t reflect.Type) (func(reflect.Value) reflect.Value, func(reflect.Value) reflect.Value) {
	key, ok := FindTaggedField(t, KeyTag)
	if !ok {
		panic("Key tag not found")
	}
	value, ok := FindTaggedField(t, ValueTag, ValuesTag)
	if !ok {
		panic("Value tag not found")
	}

	keyFn := func(v reflect.Value) reflect.Value {
		return v.FieldByIndex(key.Index)
	}
	valueFn := func(v reflect.Value) reflect.Value {
		return v.FieldByIndex(value.Index)
	}
	return keyFn, valueFn
}
