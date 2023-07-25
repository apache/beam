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
	"strings"
	"testing"
)

type mapperString struct {
	fn func(string) string
}

func mapperStringMaker(fn any) Func {
	f := fn.(func(string) string)
	return &mapperString{fn: f}
}

func (c *mapperString) Name() string {
	return "testMapperString"
}

func (c *mapperString) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *mapperString) Call(args []any) []any {
	out := c.fn(args[0].(string))
	return []any{out}
}

func (c *mapperString) Call1x1(v any) any {
	return c.fn(v.(string))
}

func TestMakeFunc(t *testing.T) {
	RegisterFunc(reflect.TypeOf((*func(string) string)(nil)).Elem(), mapperStringMaker)
	fn := func(str string) string {
		return string(str)
	}
	madeFn := MakeFunc(fn)

	if got, want := madeFn.Name(), "testMapperString"; got != want {
		t.Fatalf("MakeFunc(fn).Name()=%v, want %v", got, want)
	}
}

func TestCallNoPanic(t *testing.T) {
	RegisterFunc(reflect.TypeOf((*func(string) string)(nil)).Elem(), mapperStringMaker)
	fn := func(str string) string {
		return string(str)
	}
	madeFn := MakeFunc(fn)

	ret, err := CallNoPanic(madeFn, []any{"tester"})
	if err != nil {
		t.Fatalf("CallNoPanic(madeFn, [\"tester\"]) - unexpected error %v", err)
	}
	if got, want := ret[0].(string), string("tester"); got != want {
		t.Fatalf("CallNoPanic(madeFn, [\"tester\"]) got %v, want %v", got, want)
	}
}

func TestCallNoPanic_Panic(t *testing.T) {
	RegisterFunc(reflect.TypeOf((*func(string) string)(nil)).Elem(), mapperStringMaker)
	fn := func(str string) string {
		if str == "tester" {
			panic("OH NO!")
		}
		return string(str)
	}
	madeFn := MakeFunc(fn)

	_, err := CallNoPanic(madeFn, []any{"tester"})
	if err == nil {
		t.Fatalf("CallNoPanic(madeFn, [\"tester\"]) didn't error when it should have")
	}
	if !strings.Contains(err.Error(), "OH NO!") {
		t.Fatalf("CallNoPanic(madeFn, [\"tester\"]) error should have contained OH NO! instead returned error %v", err)
	}
}

func TestValue(t *testing.T) {
	interfaces := []any{"hi", 42, func() {}}
	want := []reflect.Kind{reflect.String, reflect.Int, reflect.Func}

	got := ValueOf(interfaces)
	if len(got) != len(want) {
		t.Fatalf("ValueOf(interfaces) got slice %v, expected slice of length %v", got, len(want))
	}
	for idx := range got {
		if got[idx].Kind() != want[idx] {
			t.Errorf("ValueOf(interfaces)[%v], got %v of kind %v, want %v", idx, got[idx], got[idx].Kind(), want[idx])
		}
	}
}

func TestInterface(t *testing.T) {
	interfaces := []any{"hi", 42}
	values := ValueOf(interfaces)
	got := Interface(values)

	if len(got) != len(interfaces) {
		t.Fatalf("Interface(values) got slice %v, expected slice of length %v", got, len(interfaces))
	}
	for idx := range got {
		if got[idx] != interfaces[idx] {
			t.Errorf("Interface(values)[%v]=%v, want %v", idx, got[idx], interfaces[idx])
		}
	}
}
