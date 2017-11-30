package userfn

import (
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/util/reflectx"
)

// IsIter returns true iff the supplied type is a "single sweep functional iterator".
//
// A single sweep functional iterator is a function taking one or more pointers
// of data as arguments, that returns a single boolean value. The semantics of
// the function are that when called, if there are values to be supplied, they
// will be copied into the supplied pointers. The function returns true if
// data was copied, and false if there is no more data available.
func IsIter(t reflect.Type) bool {
	_, ok := UnfoldIter(t)
	return ok
}

// UnfoldIter returns the parameter types, if a single sweep functional
// iterator. For example:
//
//     func (*int) bool                   returns {int}
//     func (*string, *int) bool          returns {string, int}
//     func (*typex.EventTime, *int) bool returns {typex.EventTime, int}
//
func UnfoldIter(t reflect.Type) ([]reflect.Type, bool) {
	if t.Kind() != reflect.Func {
		return nil, false
	}

	if t.NumOut() != 1 || t.Out(0) != reflectx.Bool {
		return nil, false
	}
	if t.NumIn() == 0 {
		return nil, false
	}

	var ret []reflect.Type
	skip := 0
	if t.In(0).Kind() == reflect.Ptr && t.In(0).Elem() == typex.EventTimeType {
		ret = append(ret, typex.EventTimeType)
		skip = 1
	}
	if t.NumIn()-skip > 2 || t.NumIn() == skip {
		return nil, false
	}

	for i := skip; i < t.NumIn(); i++ {
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

// IsReIter returns true iff the supplied type is a functional iterator generator.
//
// A functional iterator generator is a parameter-less function that returns
// single sweep functional iterators.
func IsReIter(t reflect.Type) bool {
	_, ok := UnfoldReIter(t)
	return ok
}

// UnfoldReIter returns the parameter types, if a functional iterator generator.
func UnfoldReIter(t reflect.Type) ([]reflect.Type, bool) {
	if t.Kind() != reflect.Func {
		return nil, false
	}
	if t.NumIn() != 0 || t.NumOut() != 1 {
		return nil, false
	}
	return UnfoldIter(t.Out(0))
}
