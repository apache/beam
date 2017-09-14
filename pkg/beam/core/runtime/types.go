package runtime

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

var types = make(map[string]reflect.Type)

// RegisterType inserts "external" types into a global type registry to bypass
// serialization and preserve full method information. It should be called in
// init() only. Returns the external key for the type.
func RegisterType(t reflect.Type) string {
	if initialized {
		panic("Init hooks have already run. Register type during init() instead.")
	}

	t = reflectx.SkipPtr(t)

	k, ok := TypeKey(t)
	if !ok {
		panic(fmt.Sprintf("invalid registration type: %v", t))
	}

	if _, exists := types[k]; exists {
		panic(fmt.Sprintf("type already registered for %v", k))
	}
	types[k] = t
	return k
}

// LookupType looks up a type in the global type registry by external key.
func LookupType(key string) (reflect.Type, bool) {
	t, ok := types[key]
	return t, ok
}

// TypeKey returns the external key of a given type. Returns false if not a
// candidate for registration.
func TypeKey(t reflect.Type) (string, bool) {
	if t.PkgPath() == "" || t.Name() == "" {
		return "", false // no pre-declared or unnamed types
	}
	return fmt.Sprintf("%v.%v", t.PkgPath(), t.Name()), true
}
