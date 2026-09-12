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

package window

import (
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

type regTsOnly struct{}

func (f *regTsOnly) AssignWindows(typex.EventTime) []typex.Window { return nil }

type regSingleElem struct{}

func (f *regSingleElem) AssignWindows(typex.EventTime, int64) []typex.Window { return nil }

type regKV struct{}

func (f *regKV) AssignWindows(typex.EventTime, string, int64) []typex.Window { return nil }

type regTooManyParams struct{}

func (f *regTooManyParams) AssignWindows(typex.EventTime, string, int64, bool) []typex.Window {
	return nil
}

type regWrongReturn struct{}

func (f *regWrongReturn) AssignWindows(typex.EventTime) typex.Window { return nil }

type regWrongFirstParam struct{}

func (f *regWrongFirstParam) AssignWindows(int64) []typex.Window { return nil }

type regNoMethod struct{}

func TestRegisterWindowFn(t *testing.T) {
	tests := []struct {
		name       string
		register   func()
		structType reflect.Type
		wantElems  []reflect.Type
		wantPanic  bool
	}{
		{
			name:       "timestamp only",
			register:   RegisterWindowFn[*regTsOnly],
			structType: reflect.TypeFor[regTsOnly](),
			wantElems:  nil,
		},
		{
			name:       "single element",
			register:   RegisterWindowFn[*regSingleElem],
			structType: reflect.TypeFor[regSingleElem](),
			wantElems:  []reflect.Type{reflect.TypeFor[int64]()},
		},
		{
			name:       "kv element",
			register:   RegisterWindowFn[*regKV],
			structType: reflect.TypeFor[regKV](),
			wantElems:  []reflect.Type{reflect.TypeFor[string](), reflect.TypeFor[int64]()},
		},
		{
			name:      "too many params",
			register:  RegisterWindowFn[*regTooManyParams],
			wantPanic: true,
		},
		{
			name:      "return is not a window slice",
			register:  RegisterWindowFn[*regWrongReturn],
			wantPanic: true,
		},
		{
			name:      "first param is not an event time",
			register:  RegisterWindowFn[*regWrongFirstParam],
			wantPanic: true,
		},
		{
			name:      "no AssignWindows method",
			register:  RegisterWindowFn[*regNoMethod],
			wantPanic: true,
		},
		{
			name:      "not a pointer to struct",
			register:  RegisterWindowFn[regTsOnly],
			wantPanic: true,
		},
		{
			name:      "already registered",
			register:  RegisterWindowFn[*regTsOnly],
			wantPanic: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			panicked := func() (p bool) {
				defer func() { p = recover() != nil }()
				tc.register()
				return
			}()

			if panicked != tc.wantPanic {
				t.Fatalf("RegisterWindowFn panicked = %v, want %v", panicked, tc.wantPanic)
			}
			if tc.wantPanic {
				return
			}
			got, ok := LookupWindowFn(tc.structType)
			if !ok {
				t.Fatal("LookupWindowFn reports the type is not registered")
			}
			if !reflect.DeepEqual(got, tc.wantElems) {
				t.Errorf("LookupWindowFn elements = %v, want %v", got, tc.wantElems)
			}
		})
	}
}
