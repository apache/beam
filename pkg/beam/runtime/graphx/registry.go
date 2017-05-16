package graphx

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/util/reflectx"
)

var types = make(map[string]reflect.Type)

// Register inserts "external" structs into a global type registry to bypass
// serialization and preserve full method information. It should be called in
// init() only.
func Register(t reflect.Type) {
	t = reflectx.SkipPtr(t)

	key, ok := Key(t)
	if !ok {
		panic(fmt.Sprintf("invalid registration type: %v", t))
	}

	if _, exists := types[key]; exists {
		panic(fmt.Sprintf("type already registered for %v", key))
	}
	types[key] = t
}

// Lookup looks up a type in the global type registry by external key.
func Lookup(key string) (reflect.Type, bool) {
	t, ok := types[key]
	return t, ok
}

// Key returns the external key of a given struct type. Returns false if not a
// candidate for registration.
func Key(t reflect.Type) (string, bool) {
	if t.Kind() != reflect.Struct {
		return "", false
	}
	return t.Name(), true
}
