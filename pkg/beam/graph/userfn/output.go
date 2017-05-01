package userfn

import (
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"reflect"
)

func IsEmit(t reflect.Type) bool {
	_, ok := UnfoldEmit(t)
	return ok
}

// UnfoldEmit returns the parameter types, if an emitter.
// For example:
//       "func (int)"
//       "func (string, T)"
//       "func (EventTime, int)"
//       "func (EventTime, string, T)"
// If there are 2 regular parameters, a KV output is implied. An optional
// evnet time is allowed as well. Emitters cannot fail.
func UnfoldEmit(t reflect.Type) ([]reflect.Type, bool) {
	if t.Kind() != reflect.Func {
		return nil, false
	}

	if t.NumOut() != 0 {
		return nil, false
	}
	if t.NumIn() == 0 {
		return nil, false
	}

	var ret []reflect.Type
	skip := 0
	if t.In(0) == typex.EventTimeType {
		ret = append(ret, typex.EventTimeType)
		skip = 1
	}
	if t.NumIn()-skip > 2 {
		return nil, false
	}

	for i := skip; i < t.NumIn(); i++ {
		if !isInParam(t.In(i)) {
			return nil, false
		}
		ret = append(ret, t.In(i))
	}
	return ret, true
}

func isInParam(t reflect.Type) bool {
	return typex.IsConcrete(t) || typex.IsUniversal(t)
}
