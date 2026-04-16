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

package batch

import (
	"reflect"
	"testing"
)

func TestDefaultElementByteSize(t *testing.T) {
	cases := []struct {
		name string
		v    any
		want int64
		ok   bool
	}{
		{"bytes_5", []byte("abcde"), 5, true},
		{"bytes_empty", []byte{}, 0, true},
		{"string_5", "abcde", 5, true},
		{"string_empty", "", 0, true},
		{"bool", true, 1, true},
		{"int8", int8(1), 1, true},
		{"uint8", uint8(1), 1, true},
		{"int16", int16(1), 2, true},
		{"uint16", uint16(1), 2, true},
		{"int32", int32(1), 4, true},
		{"uint32", uint32(1), 4, true},
		{"float32", float32(1.0), 4, true},
		{"int", int(1), 8, true},
		{"uint", uint(1), 8, true},
		{"int64", int64(1), 8, true},
		{"uint64", uint64(1), 8, true},
		{"float64", float64(1.0), 8, true},
		{"struct_unsupported", struct{ A int }{A: 1}, 0, false},
		{"map_unsupported", map[string]int{"a": 1}, 0, false},
		{"slice_int_unsupported", []int{1, 2, 3}, 0, false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, ok := defaultElementByteSize(c.v)
			if ok != c.ok {
				t.Errorf("ok = %v, want %v", ok, c.ok)
			}
			if got != c.want {
				t.Errorf("size = %d, want %d", got, c.want)
			}
		})
	}
}

func TestIsBuiltinSizeable(t *testing.T) {
	cases := []struct {
		name string
		t    reflect.Type
		want bool
	}{
		{"nil", nil, false},
		{"string", reflect.TypeOf(""), true},
		{"bytes", reflect.TypeOf([]byte(nil)), true},
		{"bool", reflect.TypeOf(true), true},
		{"int", reflect.TypeOf(int(0)), true},
		{"int64", reflect.TypeOf(int64(0)), true},
		{"float64", reflect.TypeOf(float64(0)), true},
		{"struct", reflect.TypeOf(struct{ A int }{}), false},
		{"map", reflect.TypeOf(map[string]int{}), false},
		{"slice_int", reflect.TypeOf([]int{}), false},
		{"slice_string", reflect.TypeOf([]string{}), false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := isBuiltinSizeable(c.t); got != c.want {
				t.Errorf("isBuiltinSizeable(%v) = %v, want %v", c.t, got, c.want)
			}
		})
	}
}
