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

package typex

import (
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

func TestIsBound(t *testing.T) {
	tests := []struct {
		T   FullType
		Exp bool
	}{
		{New(reflectx.Int), true},
		{New(TType), false},
		{NewCoGBK(New(TType), New(reflectx.String)), false},
		{NewCoGBK(New(reflectx.String), New(reflectx.String)), true},
		{NewKV(New(reflectx.String), New(reflect.SliceOf(reflectx.Int))), true},
		{NewKV(New(reflectx.String), New(reflect.SliceOf(XType))), false},
		{NewKV(New(reflectx.String), New(reflectx.String)), true},
		{NewKV(New(reflectx.String), New(reflect.SliceOf(reflectx.String))), true},
		{NewW(NewKV(New(reflectx.ByteSlice), New(reflectx.Int))), true},
		{New(reflect.SliceOf(KVType), NewKV(New(reflectx.ByteSlice), New(reflectx.Int))), true},
		{New(TimersType, New(reflectx.ByteSlice)), true},
	}

	for _, test := range tests {
		if actual := IsBound(test.T); actual != test.Exp {
			t.Errorf("IsBound(%v) = %v, want %v", test.T, actual, test.Exp)
		}
	}
}

func TestIsStructurallyAssignable(t *testing.T) {
	type foo int
	var f foo
	fooT := reflect.TypeOf(f)
	tests := []struct {
		A, B FullType
		Exp  bool
	}{
		{New(reflectx.Int), New(reflectx.Int), true},
		{New(reflectx.Int32), New(reflectx.Int64), false}, // from Go assignability
		{New(reflectx.Int64), New(reflectx.Int32), false}, // from Go assignability
		{New(reflectx.Int), New(TType), true},
		{New(reflectx.Int), New(fooT), false},
		{New(XType), New(TType), true},
		{NewKV(New(XType), New(YType)), New(TType), false},                                                   // T cannot match composites
		{NewKV(New(reflectx.Int), New(reflectx.Int)), NewCoGBK(New(reflectx.Int), New(reflectx.Int)), false}, // structural mismatch
		{NewKV(New(XType), New(reflectx.Int)), NewKV(New(TType), New(UType)), true},
		{NewKV(New(XType), New(reflectx.Int)), NewKV(New(TType), New(XType)), true},
		{NewKV(New(reflectx.String), New(reflectx.Int)), NewKV(New(TType), New(TType)), true},
		{NewKV(New(reflectx.Int), New(reflectx.Int)), NewKV(New(TType), New(TType)), true},
		{NewKV(New(reflectx.Int), New(reflectx.String)), NewKV(New(TType), New(reflectx.String)), true},
		{New(reflect.SliceOf(reflectx.Int)), New(reflect.SliceOf(fooT)), false},
		{New(reflect.SliceOf(reflectx.Int)), New(TType), true},
	}

	for _, test := range tests {
		if actual := IsStructurallyAssignable(test.A, test.B); actual != test.Exp {
			t.Errorf("IsStructurallyAssignable(%v -> %v) = %v, want %v", test.A, test.B, actual, test.Exp)
		}
	}
}

func TestBindSubstitute(t *testing.T) {
	tests := []struct {
		Model, In, Out, Exp FullType
	}{
		{
			New(reflectx.String),
			New(XType),
			NewKV(New(reflectx.Int), New(XType)),
			NewKV(New(reflectx.Int), New(reflectx.String)),
		},
		{
			New(reflectx.String),
			New(XType),
			NewKV(New(reflectx.Int), New(reflectx.Int)),
			NewKV(New(reflectx.Int), New(reflectx.Int)),
		},
		{
			NewKV(New(reflectx.Int), New(reflectx.String)),
			NewKV(New(XType), New(YType)),
			NewCoGBK(New(XType), New(XType)),
			NewCoGBK(New(reflectx.Int), New(reflectx.Int)),
		},
		{
			NewCoGBK(New(reflectx.Int), New(reflectx.String)),
			NewCoGBK(New(XType), New(YType)),
			NewCoGBK(New(YType), New(XType)),
			NewCoGBK(New(reflectx.String), New(reflectx.Int)),
		},
		{
			NewCoGBK(New(ZType), New(XType)),
			NewCoGBK(New(XType), New(YType)),
			NewCoGBK(New(YType), New(XType)),
			NewCoGBK(New(XType), New(ZType)),
		},
		{
			New(reflect.SliceOf(reflectx.String)),
			New(XType),
			NewKV(New(reflectx.Int), New(XType)),
			NewKV(New(reflectx.Int), New(reflect.SliceOf(reflectx.String))),
		},
		{
			New(reflectx.String),
			New(XType),
			NewKV(New(reflectx.Int), New(reflect.SliceOf(XType))),
			NewKV(New(reflectx.Int), New(reflect.SliceOf(reflectx.String))),
		},
	}

	for _, test := range tests {
		binding, err := Bind([]FullType{test.In}, []FullType{test.Model})
		if err != nil {
			t.Errorf("Bind(%v <- %v) failed: %v", test.In, test.Model, err)
			continue
		}

		actual, err := Substitute([]FullType{test.Out}, binding)
		if err != nil {
			t.Errorf("Substitute(%v, %v) failed: %v", test.Out, binding, err)
			continue
		}

		if !IsEqual(actual[0], test.Exp) {
			t.Errorf("Substitute(%v,%v) = %v, want %v", test.Out, binding, actual, test.Exp)
		}
	}
}
