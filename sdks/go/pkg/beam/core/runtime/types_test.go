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

package runtime

import (
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

type S struct {
	a int
}

type R int

var sKey string

func init() {
	s := reflect.TypeOf(&S{}) // *S
	sKey = RegisterType(s)
}

func NonAnon(S) *S { return nil }

func TestKey(t *testing.T) {
	// We extract this early since we can't hardcode the package path
	// for beam primitives. It would cause test failures if the
	// package is vendored.
	tR := reflect.TypeOf(R(2))

	tests := []struct {
		T   reflect.Type
		Key string
		Ok  bool
	}{
		{reflectx.Int, "", false},                      // predeclared type
		{reflectx.String, "", false},                   // predeclared type
		{reflect.TypeOf(struct{ A int }{}), "", false}, // unnamed struct
		{reflect.TypeOf(S{}), sKey, true},
		{reflect.TypeOf(&S{}), "", false},                            // ptr (= no name)
		{reflect.TypeOf([]S{}), "", false},                           // slice (= no name)
		{reflect.TypeOf([3]S{}), "", false},                          // array (= no name)
		{reflect.TypeOf(map[S]S{}), "", false},                       // map (= no name)
		{reflect.TypeOf(func(S) *S { return nil }), "", false},       // anon func (= no name)
		{reflect.TypeOf(NonAnon), "", false},                         // func (= no name)
		{reflect.TypeOf(R(2)), tR.PkgPath() + "." + tR.Name(), true}, // Declared non struct type (= no name)
	}

	for _, test := range tests {
		key, ok := TypeKey(test.T)
		if key != test.Key || ok != test.Ok {
			t.Errorf("TypeKey(%v) = (%v,%v), want (%v,%v)", test.T, key, ok, test.Key, test.Ok)
		}
	}
}

func TestRegister(t *testing.T) {
	s := reflect.TypeOf(&S{}) // *S

	for bad, key := range []string{"S", "graph.S", "foo", ""} {
		if _, ok := LookupType(key); ok {
			t.Fatalf("LookupType(%v) = (%v, true), want false", key, bad)
		}
	}

	actual, ok := LookupType(sKey)
	if !ok {
		t.Fatalf("LookupType(S) failed")
	}
	if actual != s.Elem() {
		t.Fatalf("LookupType(S) = %v, want %v", actual, s.Elem())
	}
}
