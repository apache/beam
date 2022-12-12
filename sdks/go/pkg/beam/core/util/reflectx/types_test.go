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

func TestIsNumber(t *testing.T) {
	i := 4
	tests := []struct {
		val any
		out bool
	}{
		{
			val: 50,
			out: true,
		},
		{
			val: int8(1),
			out: true,
		},
		{
			val: int16(2),
			out: true,
		},
		{
			val: int32(3),
			out: true,
		},
		{
			val: int64(4),
			out: true,
		},
		{
			val: uint(50),
			out: true,
		},
		{
			val: uint8(1),
			out: true,
		},
		{
			val: uint16(2),
			out: true,
		},
		{
			val: uint32(3),
			out: true,
		},
		{
			val: uint64(4),
			out: true,
		},
		{
			val: float32(3),
			out: true,
		},
		{
			val: float64(4),
			out: true,
		},
		{
			val: complex64(3),
			out: true,
		},
		{
			val: complex128(4),
			out: true,
		},
		{
			val: &i,
			out: false,
		},
		{
			val: "10",
			out: false,
		},
		{
			val: func(a int) int {
				return a
			},
			out: false,
		},
	}
	for _, test := range tests {
		if got, want := IsNumber(reflect.TypeOf(test.val)), test.out; got != want {
			t.Errorf("IsNumber(reflect.TypeOf(%v))=%v, want %v", test.val, got, want)
		}
	}
}

func TestIsInteger(t *testing.T) {
	i := 4
	tests := []struct {
		val any
		out bool
	}{
		{
			val: 50,
			out: true,
		},
		{
			val: int8(1),
			out: true,
		},
		{
			val: int16(2),
			out: true,
		},
		{
			val: int32(3),
			out: true,
		},
		{
			val: int64(4),
			out: true,
		},
		{
			val: uint(50),
			out: true,
		},
		{
			val: uint8(1),
			out: true,
		},
		{
			val: uint16(2),
			out: true,
		},
		{
			val: uint32(3),
			out: true,
		},
		{
			val: uint64(4),
			out: true,
		},
		{
			val: float32(3),
			out: false,
		},
		{
			val: float64(4),
			out: false,
		},
		{
			val: complex64(3),
			out: false,
		},
		{
			val: complex128(4),
			out: false,
		},
		{
			val: &i,
			out: false,
		},
		{
			val: "10",
			out: false,
		},
		{
			val: func(a int) int {
				return a
			},
			out: false,
		},
	}
	for _, test := range tests {
		if got, want := IsInteger(reflect.TypeOf(test.val)), test.out; got != want {
			t.Errorf("IsInteger(reflect.TypeOf(%v))=%v, want %v", test.val, got, want)
		}
	}
}

func TestIsFloat(t *testing.T) {
	i := 4
	tests := []struct {
		val any
		out bool
	}{
		{
			val: 50,
			out: false,
		},
		{
			val: int8(1),
			out: false,
		},
		{
			val: int16(2),
			out: false,
		},
		{
			val: int32(3),
			out: false,
		},
		{
			val: int64(4),
			out: false,
		},
		{
			val: uint(50),
			out: false,
		},
		{
			val: uint8(1),
			out: false,
		},
		{
			val: uint16(2),
			out: false,
		},
		{
			val: uint32(3),
			out: false,
		},
		{
			val: uint64(4),
			out: false,
		},
		{
			val: float32(3),
			out: true,
		},
		{
			val: float64(4),
			out: true,
		},
		{
			val: complex64(3),
			out: false,
		},
		{
			val: complex128(4),
			out: false,
		},
		{
			val: &i,
			out: false,
		},
		{
			val: "10",
			out: false,
		},
		{
			val: func(a int) int {
				return a
			},
			out: false,
		},
	}
	for _, test := range tests {
		if got, want := IsFloat(reflect.TypeOf(test.val)), test.out; got != want {
			t.Errorf("IsFloat(reflect.TypeOf(%v))=%v, want %v", test.val, got, want)
		}
	}
}

func TestIsComplex(t *testing.T) {
	i := 4
	tests := []struct {
		val any
		out bool
	}{
		{
			val: 50,
			out: false,
		},
		{
			val: int8(1),
			out: false,
		},
		{
			val: int16(2),
			out: false,
		},
		{
			val: int32(3),
			out: false,
		},
		{
			val: int64(4),
			out: false,
		},
		{
			val: uint(50),
			out: false,
		},
		{
			val: uint8(1),
			out: false,
		},
		{
			val: uint16(2),
			out: false,
		},
		{
			val: uint32(3),
			out: false,
		},
		{
			val: uint64(4),
			out: false,
		},
		{
			val: float32(3),
			out: false,
		},
		{
			val: float64(4),
			out: false,
		},
		{
			val: complex64(3),
			out: true,
		},
		{
			val: complex128(4),
			out: true,
		},
		{
			val: &i,
			out: false,
		},
		{
			val: "10",
			out: false,
		},
		{
			val: func(a int) int {
				return a
			},
			out: false,
		},
	}
	for _, test := range tests {
		if got, want := IsComplex(reflect.TypeOf(test.val)), test.out; got != want {
			t.Errorf("IsComplex(reflect.TypeOf(%v))=%v, want %v", test.val, got, want)
		}
	}
}

func TestSkipPtr(t *testing.T) {
	i := 4
	tests := []struct {
		val any
		out reflect.Type
	}{
		{
			val: i,
			out: Int,
		},
		{
			val: &i,
			out: Int,
		},
	}
	for _, test := range tests {
		if got, want := SkipPtr(reflect.TypeOf(test.val)), test.out; got != want {
			t.Errorf("SkipPtr(reflect.TypeOf(%v))=%v, want %v", test.val, got, want)
		}
	}
}

func TestMakeSlice(t *testing.T) {
	madeSlice := MakeSlice(String, reflect.ValueOf("test"), reflect.ValueOf("test2"))

	if got, want := madeSlice.Len(), 2; got != want {
		t.Fatalf("madeSlice.Len()=%v, want %v", got, want)
	}
	if got, want := madeSlice.Type(), reflect.TypeOf((*[]string)(nil)).Elem(); got != want {
		t.Fatalf("madeSlice.Type()=%v, want %v", got, want)
	}
	if got, want := madeSlice.Index(0).String(), "test"; got != want {
		t.Errorf("madeSlice.Index(0).String()=%v, want %v", got, want)
	}
	if got, want := madeSlice.Index(1).String(), "test2"; got != want {
		t.Errorf("madeSlice.Index(1).String()=%v, want %v", got, want)
	}
}

type typeWrapper any

func TestUnderlyingType(t *testing.T) {
	wrapper := typeWrapper("test")
	underlying := UnderlyingType(reflect.ValueOf(wrapper))
	if got, want := underlying.Type(), reflect.TypeOf((*string)(nil)).Elem(); got != want {
		t.Fatalf("UnderlyingType(reflect.ValueOf(wrapper)) returned type of %v, want %v", got, want)
	}
	if got, want := underlying.String(), "test"; got != want {
		t.Errorf("UnderlyingType(reflect.ValueOf(wrapper)).String()=%v, want %v", got, want)
	}
}
