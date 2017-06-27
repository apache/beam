// package reflectx contains a set of reflection utilities and well-known types.
package reflectx

import (
	"context"
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

	Context   = reflect.TypeOf((*context.Context)(nil)).Elem()
	Type      = reflect.TypeOf((*reflect.Type)(nil)).Elem()
	ByteSlice = reflect.TypeOf((*[]byte)(nil)).Elem()
)

// SkipPtr returns the target of a Ptr type, if a Ptr. Otherwise itself.
func SkipPtr(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		return t.Elem()
	}
	return t
}

// MakeSlice creates a slice of type []T with the given elements.
func MakeSlice(t reflect.Type, values ...reflect.Value) reflect.Value {
	ret := reflect.MakeSlice(reflect.SliceOf(t), len(values), len(values))
	for i, value := range values {
		if value.Type() != t {
			panic(fmt.Sprintf("element type is %v, want %v", value.Type(), t))
		}
		ret.Index(i).Set(value)
	}
	return ret
}
