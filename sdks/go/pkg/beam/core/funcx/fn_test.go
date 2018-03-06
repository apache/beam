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
	"strings"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

type foo struct {
	i int
}

func (m foo) Do(context.Context, int, string) (string, int, error) {
	return "", m.i, nil
}

func TestNew(t *testing.T) {
	tests := []struct {
		Name  string
		Fn    interface{}
		Param []FnParamKind
		Ret   []ReturnKind
		Err   error
	}{
		{
			Name: "niladic",
			Fn:   func() {},
		},
		{
			Name: "good1",
			Fn: func(context.Context, int, string) (typex.EventTime, string, int, error) {
				return typex.EventTime{}, "", 0, nil
			},
			Param: []FnParamKind{FnContext, FnValue, FnValue},
			Ret:   []ReturnKind{RetEventTime, RetValue, RetValue, RetError},
		},
		{
			Name:  "good2",
			Fn:    func(int, func(*int) bool, func(*int, *string) bool) {},
			Param: []FnParamKind{FnValue, FnIter, FnIter},
		},
		{
			Name: "good3",
			Fn: func(func(int, int), func(typex.EventTime, int, int), func(string), func(typex.EventTime, string)) {
			},
			Param: []FnParamKind{FnEmit, FnEmit, FnEmit, FnEmit},
		},
		{
			Name:  "good4",
			Fn:    func(typex.EventTime, reflect.Type, []byte) {},
			Param: []FnParamKind{FnEventTime, FnType, FnValue},
		},
		{
			Name:  "good-method",
			Fn:    foo{1}.Do,
			Param: []FnParamKind{FnContext, FnValue, FnValue},
			Ret:   []ReturnKind{RetValue, RetValue, RetError},
		},
		{
			Name:  "no inputs, no RetOutput",
			Fn:    func(context.Context, typex.EventTime, reflect.Type, func(int)) error { return nil },
			Param: []FnParamKind{FnContext, FnEventTime, FnType, FnEmit},
			Ret:   []ReturnKind{RetError},
		},
		{
			Name: "errContextParam: after input",
			Fn:   func(string, context.Context) {},
			Err:  errContextParam,
		},
		{
			Name: "errContextParam: after output",
			Fn:   func(string, func(string), context.Context) {},
			Err:  errContextParam,
		},
		{
			Name: "errContextParam: after EventTime",
			Fn:   func(typex.EventTime, context.Context, int) {},
			Err:  errContextParam,
		},
		{
			Name: "errContextParam: after reflect.Type",
			Fn:   func(reflect.Type, context.Context, int) {},
			Err:  errContextParam,
		},
		{
			Name: "errContextParam: multiple context",
			Fn:   func(context.Context, context.Context, int) {},
			Err:  errContextParam,
		},
		{
			Name: "errEventTimeParamPrecedence: after value",
			Fn: func(int, typex.EventTime) {
			},
			Err: errEventTimeParamPrecedence,
		},
		{
			Name: "errEventTimeParamPrecedence: after reflect type",
			Fn: func(reflect.Type, typex.EventTime, int) {
			},
			Err: errEventTimeParamPrecedence,
		},
		{
			Name: "errReflectTypePrecedence: after value",
			Fn: func(int, reflect.Type) {
			},
			Err: errReflectTypePrecedence,
		},
		{
			Name: "errReflectTypePrecedence: after reflect type",
			Fn: func(reflect.Type, reflect.Type, int) {
			},
			Err: errReflectTypePrecedence,
		},
		{
			Name: "errSideInputPrecedence- Iter before main input",
			Fn:   func(func(*int) bool, func(*int, *string) bool, int) {},
			Err:  errSideInputPrecedence,
		},
		{
			Name: "errSideInputPrecedence- ReIter before main input",
			Fn:   func(func() func(*int) bool, int) {},
			Err:  errSideInputPrecedence,
		},
		{
			Name: "errInputPrecedence- Iter before after output",
			Fn:   func(int, func(int), func(*int) bool, func(*int, *string) bool) {},
			Err:  errInputPrecedence,
		},
		{
			Name: "errInputPrecedence- ReIter before after output",
			Fn:   func(int, func(int), func() func(*int) bool) {},
			Err:  errInputPrecedence,
		},
		{
			Name: "errInputPrecedence- input after output",
			Fn:   func(int, func(int), int) {},
			Err:  errInputPrecedence,
		},
		{
			Name: "errErrorPrecedence - first",
			Fn: func() (error, string) {
				return nil, ""
			},
			Err: errErrorPrecedence,
		},
		{
			Name: "errErrorPrecedence - second",
			Fn: func() (typex.EventTime, error, string) {
				return typex.EventTime{}, nil, ""
			},
			Err: errErrorPrecedence,
		},
		{
			Name: "errEventTimeRetPrecedence",
			Fn: func() (string, typex.EventTime) {
				return "", typex.EventTime{}
			},
			Err: errEventTimeRetPrecedence,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.Name, func(t *testing.T) {
			u, err := New(reflectx.MakeFunc(test.Fn))
			if err != nil {
				if test.Err == nil {
					t.Fatalf("Expected test.Err to be set; got: New(%v) failed: %v", test.Fn, err)
				}
				if u != nil {
					t.Errorf("New(%v) failed, and returned non nil: %v, %v", test.Fn, u, err)
				}
				if strings.Contains(err.Error(), test.Err.Error()) {
					return // Received the expected error.
				}
				t.Fatalf("New(%v) failed: %v;\n\twant err to contain: \"%v\"", test.Fn, err, test.Err)
			}

			param := projectParamKind(u)
			if !reflect.DeepEqual(param, test.Param) {
				t.Errorf("New(%v).Param = %v, want %v", test.Fn, param, test.Param)
			}
			ret := projectReturnKind(u)
			if !reflect.DeepEqual(ret, test.Ret) {
				t.Errorf("New(%v).Ret = %v, want %v", test.Fn, ret, test.Ret)
			}
		})
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
