// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
		{reflectx.Float32, Concrete},
		{reflectx.Float64, Concrete},
		{reflect.TypeOf(complex64(0)), Concrete},
		{reflect.TypeOf(complex128(0)), Concrete},
		{reflect.TypeOf(struct{ A int }{}), Concrete},
		{reflect.TypeOf(struct {
			A int
			b error // ok: private interface field
		}{}), Concrete},
		{reflect.TypeOf(struct{ A []int }{}), Concrete},
		{reflect.TypeOf(reflect.Value{}), Concrete}, // ok: private fields
		{reflect.TypeOf(map[string]int{}), Concrete},
		{reflect.TypeOf(map[string]func(){}), Invalid},
		{reflect.TypeOf(map[error]int{}), Invalid},
		{reflect.TypeOf([4]int{}), Concrete},
		{reflect.TypeOf([1]string{}), Concrete},
		{reflect.TypeOf([0]string{}), Concrete},
		{reflect.TypeOf([3]struct{ Q []string }{}), Concrete},
		{reflect.TypeOf([0]interface{}{}), Concrete},
		{reflect.TypeOf([1]string{}), Concrete},
		{reflect.TypeOf([0]string{}), Concrete},
		{reflect.TypeOf([0]interface{}{}), Concrete},
		{reflect.PtrTo(reflectx.String), Concrete},
		{reflect.PtrTo(reflectx.Uint32), Concrete},
		{reflect.PtrTo(reflectx.Bool), Concrete},
		{reflect.TypeOf([4]int{}), Concrete},
		{reflect.TypeOf(&struct{ A int }{}), Concrete},
		{reflect.TypeOf(&struct{ A *int }{}), Concrete},
		{reflect.TypeOf([4]int{}), Concrete},

		// Recursive types.
		{reflect.TypeOf(RecursivePtrTest{}), Concrete},
		{reflect.TypeOf(RecursiveSliceTest{}), Concrete},
		{reflect.TypeOf(RecursivePtrArrayTest{}), Concrete},
		{reflect.TypeOf(RecursiveMapTest{}), Concrete},
		{reflect.TypeOf(RecursiveBadTest{}), Invalid},

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

		{EventTimeType, Invalid},                                     // special
		{WindowType, Invalid},                                        // special
		{reflectx.Context, Invalid},                                  // special
		{reflectx.Error, Invalid},                                    // special
		{reflect.TypeOf((*ConcreteTestWindow)(nil)).Elem(), Invalid}, // special

		{KVType, Composite},
		{CoGBKType, Composite},
		{WindowedValueType, Composite},

		{reflect.TypeOf((*interface{})(nil)).Elem(), Concrete}, // special

		{reflect.TypeOf(uintptr(0)), Invalid},          // uintptr
		{reflect.TypeOf(func() {}), Invalid},           // function
		{reflect.TypeOf(make(chan int)), Invalid},      // chan
		{reflect.TypeOf(struct{ A error }{}), Invalid}, // public interface field
	}

	for _, test := range tests {
		actual := ClassOf(test.t)
		if actual != test.exp {
			t.Errorf("ClassOf(%v) = %v, want %v", test.t, actual, test.exp)
		}
	}
}

type RecursivePtrTest struct {
	Ptr *RecursivePtrTest
}

type RecursiveSliceTest struct {
	Slice []RecursiveSliceTest
}

type RecursiveMapTest struct {
	Map map[int]RecursiveMapTest
}

// The compiler catches recursive types without indirection.
// Indirection makes recursion allowable.
type RecursivePtrArrayTest struct {
	Array [12]*RecursivePtrArrayTest
}

type RecursiveBadTest struct {
	Map map[RecursivePtrTest]*RecursiveMapTest
	Bad func()
}

type ConcreteTestWindow int

func (ConcreteTestWindow) MaxTimestamp() EventTime {
	panic("nop")
}

func (ConcreteTestWindow) Equals(o Window) bool {
	panic("nop")
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
