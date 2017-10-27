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

package funcx

import (
	"context"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

type foo struct {
	i int
}

func (m foo) Do(context.Context, int, string) (string, int, error) {
	return "", m.i, nil
}

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
		{
			Fn:    foo{1}.Do,
			Param: []FnParamKind{FnContext, FnValue, FnValue},
			Ret:   []ReturnKind{RetValue, RetValue, RetError},
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

func projectParamKind(u *Fn) []FnParamKind {
	var ret []FnParamKind
	for _, p := range u.Param {
		ret = append(ret, p.Kind)
	}
	return ret
}

func projectReturnKind(u *Fn) []ReturnKind {
	var ret []ReturnKind
	for _, p := range u.Ret {
		ret = append(ret, p.Kind)
	}
	return ret
}
