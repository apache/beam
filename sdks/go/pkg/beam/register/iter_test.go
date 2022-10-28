// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package register

import (
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
)

type myTestTypeIter1 struct {
	Int int
}

func checkRegisterations(t *testing.T, rt reflect.Type) {
	t.Helper()
	key, ok := runtime.TypeKey(rt)
	if !ok {
		t.Fatalf("runtime.TypeKey(%v): no typekey for type", rt)
	}
	if _, ok := runtime.LookupType(key); !ok {
		t.Errorf("want type %v to be available with key %q", rt, key)
	}
	if !schema.Registered(rt) {
		t.Errorf("want type %v to be registered with schemas", rt)
	}
}

func TestIter1(t *testing.T) {
	Iter1[int]()
	if !exec.IsInputRegistered(reflect.TypeOf((*func(*int) bool)(nil)).Elem()) {
		t.Fatalf("exec.IsInputRegistered(reflect.TypeOf(((*func(*int) bool)(nil)).Elem()) = false, want true")
	}

	Iter1[myTestTypeIter1]()
	if !exec.IsInputRegistered(reflect.TypeOf((*func(*int) bool)(nil)).Elem()) {
		t.Fatalf("exec.IsInputRegistered(reflect.TypeOf(((*func(*int) bool)(nil)).Elem()) = false, want true")
	}

	ttrt := reflect.TypeOf((*myTestTypeIter1)(nil)).Elem()
	checkRegisterations(t, ttrt)
}

type myTestTypeIter2A struct {
	Int int
}

type myTestTypeIter2B struct {
	Int int
}

func TestIter2(t *testing.T) {
	Iter2[int, string]()
	if !exec.IsInputRegistered(reflect.TypeOf((*func(*int, *string) bool)(nil)).Elem()) {
		t.Fatalf("exec.IsInputRegistered(reflect.TypeOf((*func(*int, *string) bool)(nil)).Elem()) = false, want true")
	}

	Iter2[myTestTypeIter2A, myTestTypeIter2B]()
	if !exec.IsInputRegistered(reflect.TypeOf((*func(*int) bool)(nil)).Elem()) {
		t.Fatalf("exec.IsInputRegistered(reflect.TypeOf(((*func(*int) bool)(nil)).Elem()) = false, want true")
	}

	ttArt := reflect.TypeOf((*myTestTypeIter2A)(nil)).Elem()
	checkRegisterations(t, ttArt)
	ttBrt := reflect.TypeOf((*myTestTypeIter2B)(nil)).Elem()
	checkRegisterations(t, ttBrt)
}

func TestIter1_Struct(t *testing.T) {
	values := []exec.FullValue{{
		Windows:   window.SingleGlobalWindow,
		Timestamp: mtime.ZeroTimestamp,
		Elm:       "one",
	}, {
		Windows:   window.SingleGlobalWindow,
		Timestamp: mtime.ZeroTimestamp,
		Elm:       "two",
	}, {
		Windows:   window.SingleGlobalWindow,
		Timestamp: mtime.ZeroTimestamp,
		Elm:       "three",
	}}

	i := iter1[string]{s: &exec.FixedReStream{Buf: values}}

	i.Init()
	fn := i.Value().(func(value *string) bool)

	var s string
	if ok := fn(&s); !ok {
		t.Fatalf("First i.Value()(&s)=false, want true")
	}
	if got, want := s, "one"; got != want {
		t.Fatalf("First iter value = %v, want %v", got, want)
	}
	if ok := fn(&s); !ok {
		t.Fatalf("Second i.Value()(&s)=false, want true")
	}
	if got, want := s, "two"; got != want {
		t.Fatalf("Second iter value = %v, want %v", got, want)
	}
	if ok := fn(&s); !ok {
		t.Fatalf("Third i.Value()(&s)=false, want true")
	}
	if got, want := s, "three"; got != want {
		t.Fatalf("Third iter value = %v, want %v", got, want)
	}
	if ok := fn(&s); ok {
		t.Fatalf("Fourth i.Value()(&s)=true, want false")
	}
	if err := i.Reset(); err != nil {
		t.Fatalf("i.Reset()=%v, want nil", err)
	}
}

func TestIter2_Struct(t *testing.T) {
	values := []exec.FullValue{{
		Windows:   window.SingleGlobalWindow,
		Timestamp: mtime.ZeroTimestamp,
		Elm:       1,
		Elm2:      "one",
	}, {
		Windows:   window.SingleGlobalWindow,
		Timestamp: mtime.ZeroTimestamp,
		Elm:       2,
		Elm2:      "two",
	}, {
		Windows:   window.SingleGlobalWindow,
		Timestamp: mtime.ZeroTimestamp,
		Elm:       3,
		Elm2:      "three",
	}}

	i := iter2[int, string]{s: &exec.FixedReStream{Buf: values}}

	i.Init()
	fn := i.Value().(func(key *int, value *string) bool)

	var s string
	var key int
	if ok := fn(&key, &s); !ok {
		t.Fatalf("First i.Value()(&s)=false, want true")
	}
	if got, want := key, 1; got != want {
		t.Fatalf("First iter key = %v, want %v", got, want)
	}
	if got, want := s, "one"; got != want {
		t.Fatalf("First iter value = %v, want %v", got, want)
	}
	if ok := fn(&key, &s); !ok {
		t.Fatalf("Second i.Value()(&s)=false, want true")
	}
	if got, want := key, 2; got != want {
		t.Fatalf("Second iter key = %v, want %v", got, want)
	}
	if got, want := s, "two"; got != want {
		t.Fatalf("Second iter value = %v, want %v", got, want)
	}
	if ok := fn(&key, &s); !ok {
		t.Fatalf("Third i.Value()(&s)=false, want true")
	}
	if got, want := key, 3; got != want {
		t.Fatalf("Third iter key = %v, want %v", got, want)
	}
	if got, want := s, "three"; got != want {
		t.Fatalf("Third iter value = %v, want %v", got, want)
	}
	if ok := fn(&key, &s); ok {
		t.Fatalf("Fourth i.Value()(&s)=true, want false")
	}
	if err := i.Reset(); err != nil {
		t.Fatalf("i.Reset()=%v, want nil", err)
	}
}
