package runtime

import (
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

type S struct {
	a int
}

func TestKey(t *testing.T) {
	tests := []struct {
		T   reflect.Type
		Key string
		Ok  bool
	}{
		{reflectx.Int, "", false},                      // predeclared type
		{reflectx.String, "", false},                   // predeclared type
		{reflect.TypeOf(struct{ A int }{}), "", false}, // unnamed struct
		{reflect.TypeOf(S{}), "github.com/apache/beam/sdks/go/pkg/beam/core/runtime.S", true},
		{reflect.TypeOf(&S{}), "", false},  // ptr (= no name)
		{reflect.TypeOf([]S{}), "", false}, // slice (= no name)
	}

	for _, test := range tests {
		key, ok := TypeKey(test.T)
		if key != test.Key || ok != test.Ok {
			t.Errorf("TypeKey(%v) = (%v,%v), want (%v,%v)", test.T, key, ok, test.Key, test.Ok)
		}
	}
}

func TestRegister(t *testing.T) {
	s := reflect.TypeOf(&S{}) // *S

	RegisterType(s)

	for bad, key := range []string{"S", "graph.S", "foo", ""} {
		if _, ok := LookupType(key); ok {
			t.Fatalf("LookupType(%v) = (%v, true), want false", key, bad)
		}
	}

	actual, ok := LookupType("github.com/apache/beam/sdks/go/pkg/beam/core/runtime.S")
	if !ok {
		t.Fatalf("LookupType(S) failed")
	}
	if actual != s.Elem() {
		t.Fatalf("LookupType(S) = %v, want %v", actual, s.Elem())
	}
}
