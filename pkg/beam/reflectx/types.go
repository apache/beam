// package reflectx contains a set of reflection utilities and well-known types.
package reflectx

import "reflect"

// Well-known reflected types. Convenience definitions.
var (
	Int    = reflect.TypeOf((*int)(nil)).Elem()
	String = reflect.TypeOf((*string)(nil)).Elem()
	Error  = reflect.TypeOf((*error)(nil)).Elem()
)

// SkipPtr returns the target of a Ptr type, if a Ptr. Otherwise itself.
func SkipPtr(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		return t.Elem()
	}
	return t
}
