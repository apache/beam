package userfn

import (
	"context"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"reflect"
	"testing"
)

func TestNew(t *testing.T) {
	tests := []struct {
		Fn    interface{}
		Param []FnParamKind
		Ret   []ReturnKind
	}{
		{
			Fn: func() {},
		},
		{
			Fn:    func(context.Context, int, string) (string, int, error) { return "", 0, nil },
			Param: []FnParamKind{FnContext, FnValue, FnValue},
			Ret:   []ReturnKind{RetValue, RetValue, RetError},
		},
		{
			Fn:    func(func(*int) bool, func(*int, *string) bool) {},
			Param: []FnParamKind{FnIter, FnIter},
		},
		{
			Fn:    func(func(int, int), func(typex.EventTime, int, int), func(string), func(typex.EventTime, string)) {},
			Param: []FnParamKind{FnEmit, FnEmit, FnEmit, FnEmit},
		},
		{
			Fn:    func(reflect.Type, typex.EventTime, []byte) {},
			Param: []FnParamKind{FnType, FnEventTime, FnValue},
		},
	}

	for _, test := range tests {
		u, err := New(test.Fn)
		if err != nil {
			t.Fatalf("New(%v) failed: %v", test.Fn, err)
		}

		param := projectParamKind(u)
		if !reflect.DeepEqual(param, test.Param) {
			t.Errorf("New(%v).Param = %v, want %v", test.Fn, param, test.Param)
		}
		ret := projectReturnKind(u)
		if !reflect.DeepEqual(ret, test.Ret) {
			t.Errorf("New(%v).Ret = %v, want %v", test.Fn, ret, test.Ret)
		}
	}
}

func projectParamKind(u *UserFn) []FnParamKind {
	var ret []FnParamKind
	for _, p := range u.Param {
		ret = append(ret, p.Kind)
	}
	return ret
}

func projectReturnKind(u *UserFn) []ReturnKind {
	var ret []ReturnKind
	for _, p := range u.Ret {
		ret = append(ret, p.Kind)
	}
	return ret
}
