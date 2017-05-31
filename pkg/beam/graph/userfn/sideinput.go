package userfn

import (
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/util/reflectx"
)

// A single sweep functional iterator is a function
// taking one or more pointers of data as arguments, that returns a single
// boolean value. The semantics of the function are that when called, if there
// are values to be supplied, they will be copied into the supplied pointers. The
// function returns true if data was copied, and false if there is no more data
// available.

// IsIter returns true iff the supplied type is a single sweep functional iterator.
func IsIter(t reflect.Type) bool {
	_, ok := UnfoldIter(t)
	return ok
}

// UnfoldIter returns the parameter types, if a single sweep functional iterator
// of one or two parameters. For example:
func UnfoldIter(t reflect.Type) ([]reflect.Type, bool) {
	if t.Kind() != reflect.Func {
		return nil, false
	}

	if t.NumOut() != 1 || t.Out(0) != reflectx.Bool {
		return nil, false
	}
	if t.NumIn() < 1 || t.NumIn() > 2 {
		return nil, false
	}

	var ret []reflect.Type
	for i := 0; i < t.NumIn(); i++ {
		if !isOutParam(t.In(i)) {
			return nil, false
		}
		ret = append(ret, t.In(i).Elem())
	}
	return ret, true
}

func isOutParam(t reflect.Type) bool {
	if t.Kind() != reflect.Ptr {
		return false
	}
	return typex.IsConcrete(t.Elem()) || typex.IsUniversal(t.Elem()) || typex.IsContainer(t.Elem())
}
