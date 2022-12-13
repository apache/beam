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

package reflectx

import (
	"reflect"
	"testing"
)

type nameFn struct {
	Name string `json:"name,omitempty"`
}

func (f *nameFn) PrintName() string {
	return f.Name
}

func NameFnMaker(fn any) map[string]Func {
	dfn := fn.(*nameFn)
	return map[string]Func{
		"PrintName": MakeFunc(func() string { return dfn.PrintName() }),
	}
}

func TestWrapMethods_Registered(t *testing.T) {
	RegisterStructWrapper(reflect.TypeOf((*nameFn)(nil)).Elem(), NameFnMaker)
	wrapper, exists := WrapMethods(&nameFn{Name: "testName"})

	if got, want := exists, true; got != want {
		t.Errorf("WrapMethods(&nameFn{Name: \"testName\"}), nameFn registered = %v, want %v", got, want)
	}
	fn, ok := wrapper["PrintName"]
	if ok != true {
		t.Errorf("WrapMethods(&nameFn{Name: \"testName\"}) doesn't contain PrintName, want a function")
	}
	if got, want := fn.Call([]any{})[0].(string), "testName"; got != want {
		t.Errorf("WrapMethods(&nameFn{Name: \"testName\"}) invoked PrintName, got %v, want %v", got, want)
	}
}

type unregisteredFn struct {
	Name string `json:"name,omitempty"`
}

func TestWrapMethods_Unregistered(t *testing.T) {
	wrapper, exists := WrapMethods(&unregisteredFn{Name: "testName"})

	if got, want := exists, false; got != want {
		t.Fatalf("WrapMethods(&unregisteredFn{Name: \"testName\"}), unregisteredFn registered = %v, want %v", got, want)
	}
	if got := wrapper; got != nil {
		t.Fatalf("WrapMethods(&unregisteredFn{Name: \"testName\"}), wrapper = %v, want nil", got)
	}
}
