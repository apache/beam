package typex

import (
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

// TestClassOf tests that the type classification is correct.
func TestClassOf(t *testing.T) {
	tests := []struct {
		t   reflect.Type
		exp Class
	}{
		{reflectx.Bool, Concrete},
		{reflectx.Int, Concrete},
		{reflectx.Int8, Concrete},
		{reflectx.Int16, Concrete},
		{reflectx.Int32, Concrete},
		{reflectx.Int64, Concrete},
		{reflectx.Uint, Concrete},
		{reflectx.Uint8, Concrete},
		{reflectx.Uint16, Concrete},
		{reflectx.Uint32, Concrete},
		{reflectx.Uint64, Concrete},
		{reflectx.String, Concrete},
		{reflect.TypeOf(struct{ A int }{}), Concrete},
		{reflect.TypeOf(struct {
			A int
			b error // ok: private interface field
		}{}), Concrete},
		{reflect.TypeOf(struct{ A []int }{}), Concrete},
		{reflect.TypeOf(reflect.Value{}), Concrete}, // ok: private fields

		{reflect.TypeOf([]X{}), Container},
		{reflect.TypeOf([][][]X{}), Container},
		{reflect.TypeOf([]int{}), Container},
		{reflect.TypeOf([][][]uint16{}), Container},

		{TType, Universal},
		{UType, Universal},
		{VType, Universal},
		{WType, Universal},
		{XType, Universal},
		{YType, Universal},
		{ZType, Universal},

		{KVType, Composite},
		{GBKType, Composite},
		{CoGBKType, Composite},
		{WindowedValueType, Composite},

		{reflect.TypeOf((*interface{})(nil)).Elem(), Invalid}, // empty interface
		{reflectx.Context, Invalid},                           // interface
		{reflectx.Error, Invalid},                             // interface
		{reflect.TypeOf(func() {}), Invalid},                  // function
		{reflect.TypeOf(make(chan int)), Invalid},             // chan
		{reflect.TypeOf(struct{ A error }{}), Invalid},        // public interface field
	}

	for _, test := range tests {
		actual := ClassOf(test.t)
		if actual != test.exp {
			t.Errorf("ClassOf(%v) = %v, want %v", test.t, actual, test.exp)
		}
	}
}

// TestIsConcrete tests that concrete container types, such as []int but not
// []T, are also treated as concrete under IsConcrete.
func TestIsConcrete(t *testing.T) {
	tests := []struct {
		t   reflect.Type
		exp bool
	}{
		{reflect.TypeOf([]int{}), true},
		{reflect.TypeOf([][][]uint16{}), true},
		{reflect.TypeOf([]Y{}), false},
		{reflect.TypeOf([][][]Z{}), false},
	}

	for _, test := range tests {
		actual := IsConcrete(test.t)
		if actual != test.exp {
			t.Errorf("IsConcrete(%v) = %v, want %v", test, actual, test.exp)
		}
	}

}
