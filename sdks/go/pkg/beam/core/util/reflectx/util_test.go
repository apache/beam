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
	"fmt"
	"reflect"
	"testing"
)

type foo struct {
	N int
}

func (f *foo) String() string {
	return fmt.Sprintf("&foo{%v}", f.N)
}

func TestMergeMaps(t *testing.T) {
	tests := []struct {
		a, b, exp interface{}
	}{
		{
			a:   map[string]*foo{"a": {1}},
			b:   map[string]*foo{"a": {2}},
			exp: map[string]*foo{"a": {2}},
		},
		{
			a:   map[string]*foo{"a": {1}},
			b:   nil,
			exp: map[string]*foo{"a": {1}},
		},
		{
			a:   map[string]*foo{"a": {1}},
			b:   map[string]*foo{"b": {1}},
			exp: map[string]*foo{"a": {1}, "b": {1}},
		},
		{
			a:   map[string]*foo{"a": {1}},
			b:   map[string]*foo{"a": nil, "b": {1}},
			exp: map[string]*foo{"b": {1}},
		},
		{
			a:   map[string]*foo{"a": {1}},
			b:   map[string]*foo{"a": {2}, "not_present": nil},
			exp: map[string]*foo{"a": {2}},
		},
	}

	for _, test := range tests {
		orig := fmt.Sprintf("%v", test.a) // print before mutation

		UpdateMap(test.a, test.b)
		if !reflect.DeepEqual(test.a, test.exp) {
			t.Errorf("UpdateMap(%v,%v) = %v, want %v", orig, test.b, test.a, test.exp)
		}
	}
}

func TestShallowClone(t *testing.T) {
	tests := []interface{}{
		nil,
		2,
		foo{4},
		&foo{23},
		[]string{},
		[]string{"a", "c", "e"},
		map[string]string{},
		map[string]string{"a": "c", "e": "g"},
	}
	for _, test := range tests {
		actual := ShallowClone(test)
		if !reflect.DeepEqual(actual, test) {
			t.Errorf("ShallowClone(%v) = %v, want id", test, actual)
		}
	}
}

func TestShallowCloneNil(t *testing.T) {
	var a []string

	ac := ShallowClone(a)
	if !reflect.DeepEqual(ac, a) {
		t.Errorf("ShallowClone(%v) = %v, want id", a, ac)
	}

	var b map[string]string

	bc := ShallowClone(b)
	if !reflect.DeepEqual(bc, b) {
		t.Errorf("ShallowClone(%v) = %v, want id", b, bc)
	}
}
