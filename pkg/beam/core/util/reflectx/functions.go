package reflectx

import (
	"fmt"
	"reflect"
	"runtime"
	"unsafe"
)

// TODO(herohde) 7/21/2017: we rely on the happy fact that the function name is
// also the symbol name. We should perhaps make that connection explicit.

// FunctionName returns the symbol name of a function. It panics if the given
// value is not a function.
func FunctionName(fn interface{}) string {
	val := reflect.ValueOf(fn)
	if val.Kind() != reflect.Func {
		panic(fmt.Sprintf("value %v is not a function", fn))
	}

	return runtime.FuncForPC(uintptr(val.Pointer())).Name()
}

// LoadFunction loads a function from a pointer and type. Assumes the pointer
// points to a valid function implementation.
func LoadFunction(ptr uintptr, t reflect.Type) interface{} {
	v := reflect.New(t).Elem()
	*(*uintptr)(unsafe.Pointer(v.Addr().Pointer())) = (uintptr)(unsafe.Pointer(&ptr))
	return v.Interface()
}
