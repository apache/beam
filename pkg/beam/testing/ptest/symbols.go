package ptest

import (
	"fmt"
	"reflect"
	"runtime"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
)

var symbolResolver = &resolver{registry: make(map[string]uintptr)}

func init() {
	// Setup manual symbol resolution for unit tests, where there dwarf
	// tables are not present. We automatically register the default
	// universal coders for convenience.
	graphx.SymbolResolver = symbolResolver

	RegisterFn(beam.JSONEnc)
	RegisterFn(beam.JSONDec)
}

type resolver struct {
	registry map[string]uintptr
}

// Register registers function for serialization in tests.
func (r *resolver) Register(fn interface{}) {
	val := reflect.ValueOf(fn)
	if val.Kind() != reflect.Func {
		panic(fmt.Sprintf("not a function: %v", val))
	}

	ptr := reflect.ValueOf(fn).Pointer()
	name := runtime.FuncForPC(ptr).Name()
	r.registry[name] = ptr
}

func (r *resolver) Sym2Addr(name string) (uintptr, error) {
	ptr, ok := r.registry[name]
	if !ok {
		return 0, fmt.Errorf("%v not registered. Use ptest.RegisterFn in unit tests.", name)
	}
	return ptr, nil
}

// RegisterFn is needed for functions serialized during unit tests, in
// particular custom coders.
func RegisterFn(fn interface{}) {
	symbolResolver.Register(fn)
}
