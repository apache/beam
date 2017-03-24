// package reflectx contains a set of reflection utilities and well-known types.
package reflectx

import (
	"fmt"
	"reflect"
)

// Well-known reflected types. Convenience definitions.
var (
	Bool   = reflect.TypeOf((*bool)(nil)).Elem()
	Int    = reflect.TypeOf((*int)(nil)).Elem()
	Int8   = reflect.TypeOf((*int8)(nil)).Elem()
	Int16  = reflect.TypeOf((*int16)(nil)).Elem()
	Int32  = reflect.TypeOf((*int32)(nil)).Elem()
	Int64  = reflect.TypeOf((*int64)(nil)).Elem()
	Uint   = reflect.TypeOf((*uint)(nil)).Elem()
	Uint8  = reflect.TypeOf((*uint8)(nil)).Elem()
	Uint16 = reflect.TypeOf((*uint16)(nil)).Elem()
	Uint32 = reflect.TypeOf((*uint32)(nil)).Elem()
	Uint64 = reflect.TypeOf((*uint64)(nil)).Elem()
	String = reflect.TypeOf((*string)(nil)).Elem()
	Error  = reflect.TypeOf((*error)(nil)).Elem()

	ByteSlice    = reflect.TypeOf((*[]byte)(nil)).Elem()
	ReflectValue = reflect.TypeOf((*reflect.Value)(nil)).Elem()
	ReflectType  = reflect.TypeOf((*reflect.Type)(nil)).Elem()
)

// SkipPtr returns the target of a Ptr type, if a Ptr. Otherwise itself.
func SkipPtr(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		return t.Elem()
	}
	return t
}

// MakeKV returns a synthetic KV type.
func MakeKV(key, value reflect.Type) (reflect.Type, error) {
	if ClassOf(key) == Invalid {
		return nil, fmt.Errorf("Key not valid type: %v", key)
	}
	if ClassOf(value) == Invalid {
		return nil, fmt.Errorf("Value not valid type: %v", value)
	}

	return reflect.StructOf([]reflect.StructField{
		{Name: "Key", Tag: `beam:"key"`, Type: key},
		{Name: "Value", Tag: `beam:"value"`, Type: value},
	}), nil
}

// IsKV returns true iff the type is a key-value type.
func IsKV(t reflect.Type) bool {
	_, _, ok := UnfoldKV(t)
	return ok
}

// UnfoldKV returns (T', T'', true) if the type is of the form:
//
//    type T struct {
//          K T'  `beam:"key"`
//          V T'' `beam:"value"`
//    }
//
// Note that each component of the KV can be of different classes.
func UnfoldKV(t reflect.Type) (reflect.Type, reflect.Type, bool) {
	if t.Kind() != reflect.Struct || t.NumField() != 2 {
		return nil, nil, false
	}

	key, ok := FindTaggedField(t, KeyTag)
	if !ok {
		return nil, nil, false
	}
	value, ok := FindTaggedField(t, ValueTag)
	if !ok {
		return nil, nil, false
	}
	return key.Type, value.Type, true
}

// MakeGBK returns a synthetic GBK result type.
func MakeGBK(key, value reflect.Type) (reflect.Type, error) {
	if ClassOf(key) == Invalid {
		return nil, fmt.Errorf("Key not valid type: %v", key)
	}
	if ClassOf(value) == Invalid {
		return nil, fmt.Errorf("Value not valid type: %v", value)
	}

	return reflect.StructOf([]reflect.StructField{
		{Name: "Key", Tag: `beam:"key"`, Type: key},
		{Name: "Values", Tag: `beam:"values"`, Type: reflect.ChanOf(reflect.BothDir, value)},
	}), nil
}

// IsGBK returns true iff the type is a key-values type from a GBK result.
func IsGBK(t reflect.Type) bool {
	_, _, ok := UnfoldGBK(t)
	return ok
}

// UnfoldGBK returns (T', T'', true) if the type is of the form:
//
//    type T struct {
//          K T'       `beam:"key"`
//          V chan T'' `beam:"values"`
//    }
func UnfoldGBK(t reflect.Type) (reflect.Type, reflect.Type, bool) {
	if t.Kind() != reflect.Struct || t.NumField() != 2 {
		return nil, nil, false
	}

	key, ok := FindTaggedField(t, KeyTag)
	if !ok {
		return nil, nil, false
	}
	values, ok := FindTaggedField(t, ValuesTag)
	if !ok {
		return nil, nil, false
	}

	if values.Type.Kind() != reflect.Chan {
		return nil, nil, false
	}
	return key.Type, values.Type.Elem(), true
}

// TODO(herohde): CoGBK
