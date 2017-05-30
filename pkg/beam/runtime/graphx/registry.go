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

	k, ok := key(t)
	if !ok {
		panic(fmt.Sprintf("invalid registration type: %v", t))
	}

	if _, exists := types[k]; exists {
		panic(fmt.Sprintf("type already registered for %v", k))
	}
	types[k] = t
}

// lookup looks up a type in the global type registry by external key.
func lookup(key string) (reflect.Type, bool) {
	t, ok := types[key]
	return t, ok
}

// key returns the external key of a given type. Returns false if not a
// candidate for registration.
func key(t reflect.Type) (string, bool) {
	// TODO(wcn): determine what prohibitions, if any, apply to
	// the input type. For example, maybe we shouldn't allow built-in
	// types since methods can't be attached to them.
	return t.Name(), true
}
