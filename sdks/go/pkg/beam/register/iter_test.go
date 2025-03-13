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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

type myTestTypeIter1 struct {
	Int int
}

func checkRegisterations(t *testing.T, ort reflect.Type) {
	t.Helper()
	// Strip pointers for the original type since type key doesn't support them.
	// Pointer handling is done elsewhere.
	rt := reflectx.SkipPtr(ort)
	key, ok := runtime.TypeKey(rt)
	if !ok {
		t.Fatalf("runtime.TypeKey(%v): no typekey for type", rt)
	}
	if _, ok := runtime.LookupType(key); !ok {
		t.Errorf("want type %v to be available with key %q", rt, key)
	}
	if !schema.Registered(ort) {
		t.Errorf("want type %v to be registered with schemas", ort)
	}
}

func TestIter1(t *testing.T) {
	Iter1[int]()
	itiT := reflect.TypeOf((*func(*int) bool)(nil)).Elem()
	if !exec.IsInputRegistered(itiT) {
		t.Fatalf("exec.IsInputRegistered(%v) = false, want true", itiT)
	}

	Iter1[myTestTypeIter1]()
	it1T := reflect.TypeOf((*func(*int) bool)(nil)).Elem()
	if !exec.IsInputRegistered(it1T) {
		t.Fatalf("exec.IsInputRegistered(%v) = false, want true", it1T)
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
	it2isT := reflect.TypeOf((*func(*int, *string) bool)(nil)).Elem()
	if !exec.IsInputRegistered(it2isT) {
		t.Fatalf("exec.IsInputRegistered(%v) = false, want true", it2isT)
	}

	Iter2[myTestTypeIter2A, *myTestTypeIter2B]()
	it2ABT := reflect.TypeOf((*func(*myTestTypeIter2A, **myTestTypeIter2B) bool)(nil)).Elem()
	if !exec.IsInputRegistered(it2ABT) {
		t.Fatalf("exec.IsInputRegistered(%v) = false, want true", it2ABT)
	}

	ttArt := reflect.TypeOf((*myTestTypeIter2A)(nil)).Elem()
	checkRegisterations(t, ttArt)
	ttBrt := reflect.TypeOf((*myTestTypeIter2B)(nil))
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
