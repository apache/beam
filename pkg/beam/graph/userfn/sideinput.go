package userfn

import (
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/util/reflectx"
	"reflect"
)

// TODO(herohde) 4/14/2017: Can side input have timestamps or windows?

func IsIter(t reflect.Type) bool {
	_, ok := UnfoldIter(t)
	return ok
}

// UnfoldIter returns the parameter types, if a single sweep functional iterator
// of one or two parameters. For example:
//       "func (*int) bool"
//       "func (*string, *T) bool"
// If there are 2 parameters, a KV input is implied.
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
	return typex.IsConcrete(t.Elem()) || typex.IsUniversal(t.Elem())
}
