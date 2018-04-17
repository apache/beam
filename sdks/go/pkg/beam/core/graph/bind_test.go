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

package graph

import (
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

func TestBind(t *testing.T) {
	tests := []struct {
		In  []typex.FullType // Incoming Node type
		Fn  interface{}
		Out []typex.FullType // Outgoing Node type; nil == cannot bind
	}{
		{ // Direct
			[]typex.FullType{typex.New(reflectx.Int)},
			func(int) int { return 0 },
			[]typex.FullType{typex.New(reflectx.Int)},
		},
		{ // Direct w/ KV out
			[]typex.FullType{typex.New(reflectx.Int)},
			func(int) (int, string) { return 0, "" },
			[]typex.FullType{typex.NewKV(typex.New(reflectx.Int), typex.New(reflectx.String))},
		},
		{ // KV Emitter
			[]typex.FullType{typex.New(reflectx.Int)},
			func(int, func(int, string)) {},
			[]typex.FullType{typex.NewKV(typex.New(reflectx.Int), typex.New(reflectx.String))},
		},
		{ // Direct with optionals time/error
			[]typex.FullType{typex.New(reflectx.Int)},
			func(typex.EventTime, int) (typex.EventTime, int, error) { return mtime.ZeroTimestamp, 0, nil },
			[]typex.FullType{typex.New(reflectx.Int)},
		},
		{ // Emitter w/ optionals
			[]typex.FullType{typex.New(reflectx.Int)},
			func(typex.EventTime, int, func(typex.EventTime, int)) error { return nil },
			[]typex.FullType{typex.New(reflectx.Int)},
		},
		{
			[]typex.FullType{typex.New(reflectx.Int)},
			func(string) int { return 0 },
			nil, // int cannot bind to string
		},
		{
			[]typex.FullType{typex.New(reflectx.Int)},
			func(int, int) int { return 0 },
			nil, // int cannot bind to int x int
		},
		{ // Generic
			[]typex.FullType{typex.New(reflectx.Int)},
			func(x typex.X) typex.X { return x },
			[]typex.FullType{typex.New(reflectx.Int)},
		},
		{
			[]typex.FullType{typex.NewKV(typex.New(reflectx.Int), typex.New(reflectx.String))},
			func(x typex.X) typex.X { return x },
			nil, // structural mismatch
		},
		{ // Generic swap
			[]typex.FullType{typex.NewKV(typex.New(reflectx.Int), typex.New(reflectx.String))},
			func(x typex.X, y typex.Y) (typex.Y, typex.X) { return y, x },
			[]typex.FullType{typex.NewKV(typex.New(reflectx.String), typex.New(reflectx.Int))},
		},
		{ // Side input (as singletons)
			[]typex.FullType{typex.New(reflectx.Int8), typex.New(reflectx.Int16), typex.New(reflectx.Int32)},
			func(int8, int16, int32, func(string, int)) {},
			[]typex.FullType{typex.NewKV(typex.New(reflectx.String), typex.New(reflectx.Int))},
		},
		{ // Side input (as slice and iter)
			[]typex.FullType{typex.New(reflectx.Int8), typex.New(reflectx.Int16), typex.New(reflectx.Int32)},
			func(int8, []int16, func(*int32) bool, func(int8, []int16)) {},
			[]typex.FullType{typex.NewKV(typex.New(reflectx.Int8), typex.New(reflect.SliceOf(reflectx.Int16)))},
		},
		{ // Generic side input (as iter and re-iter)
			[]typex.FullType{typex.New(reflectx.Int8), typex.New(reflectx.Int16), typex.New(reflectx.Int32)},
			func(typex.X, func(*typex.Y) bool, func() func(*typex.T) bool, func(typex.X, []typex.Y)) {},
			[]typex.FullType{typex.NewKV(typex.New(reflectx.Int8), typex.New(reflect.SliceOf(reflectx.Int16)))},
		},
		{ // Generic side output
			[]typex.FullType{typex.New(reflectx.Int8), typex.New(reflectx.Int16), typex.New(reflectx.Int32)},
			func(typex.X, typex.Y, typex.Z, func(typex.X, []typex.Y), func(int), func(typex.Z)) {},
			[]typex.FullType{typex.NewKV(typex.New(reflectx.Int8), typex.New(reflect.SliceOf(reflectx.Int16))), typex.New(reflectx.Int), typex.New(reflectx.Int32)},
		},
		{ // Bind as (K, V) ..
			[]typex.FullType{typex.NewKV(typex.New(reflectx.Int8), typex.New(reflectx.Int16))},
			func(int8, int16) int { return 0 },
			[]typex.FullType{typex.New(reflectx.Int)},
		},
		{ // .. bind same input as (V, SI) ..
			[]typex.FullType{typex.New(reflectx.Int8), typex.New(reflectx.Int16)},
			func(int8, int16) int { return 0 },
			[]typex.FullType{typex.New(reflectx.Int)},
		},
		{ // .. and allow other SI forms ..
			[]typex.FullType{typex.New(reflectx.Int8), typex.New(reflectx.Int16)},
			func(int8, func(*int16) bool) int { return 0 },
			[]typex.FullType{typex.New(reflectx.Int)},
		},
		{ // .. which won't work as (K, V).
			[]typex.FullType{typex.NewKV(typex.New(reflectx.Int8), typex.New(reflectx.Int16))},
			func(int8, func(*int16) bool) int { return 0 },
			nil,
		},
		{ // GBK binding
			[]typex.FullType{typex.NewCoGBK(typex.New(reflectx.Int8), typex.New(reflectx.Int16))},
			func(int8, func(*int16) bool) int { return 0 },
			[]typex.FullType{typex.New(reflectx.Int)},
		},
		{ // CoGBK binding
			[]typex.FullType{typex.NewCoGBK(typex.New(reflectx.Int8), typex.New(reflectx.Int16), typex.New(reflectx.Int32))},
			func(int8, func(*int16) bool, func(*int32) bool) int { return 0 },
			[]typex.FullType{typex.New(reflectx.Int)},
		},
		{ // GBK binding with side input
			[]typex.FullType{typex.NewCoGBK(typex.New(reflectx.Int8), typex.New(reflectx.Int16)), typex.New(reflectx.Int32)},
			func(int8, func(*int16) bool, func(*int32) bool) int { return 0 },
			[]typex.FullType{typex.New(reflectx.Int)},
		},
	}

	for _, test := range tests {
		fn, err := funcx.New(reflectx.MakeFunc(test.Fn))
		if err != nil {
			t.Errorf("Invalid Fn: %v", err)
			continue
		}
		_, _, _, actual, err := Bind(fn, nil, test.In...)
		if err != nil {
			if test.Out == nil {
				continue // expected
			}
			t.Errorf("Bind(%v, %v) failed: %v", fn, test.In, err)
			continue
		}

		if !typex.IsEqualList(actual, test.Out) {
			t.Errorf("Bind(%v, %v) = %v, want %v", fn, test.In, actual, test.Out)
		}
	}
}

func TestBindWithTypedefs(t *testing.T) {
	tests := []struct {
		In      []typex.FullType // Incoming Node type
		Typedef map[string]reflect.Type
		Fn      interface{}
		Out     []typex.FullType // Outgoing Node type; nil == cannot bind
	}{
		{ // Typedefs are ignored, if not used
			[]typex.FullType{typex.New(reflectx.Int)},
			map[string]reflect.Type{"X": reflectx.Int},
			func(int) int { return 0 },
			[]typex.FullType{typex.New(reflectx.Int)},
		},
		{
			nil,
			map[string]reflect.Type{"X": reflectx.Int},
			func() typex.X { return nil },
			[]typex.FullType{typex.New(reflectx.Int)},
		},
		{
			nil,
			nil,
			func() typex.X { return nil },
			nil, // X not defined
		},
	}

	for _, test := range tests {
		fn, err := funcx.New(reflectx.MakeFunc(test.Fn))
		if err != nil {
			t.Errorf("Invalid Fn: %v", err)
			continue
		}
		_, _, _, actual, err := Bind(fn, test.Typedef, test.In...)
		if err != nil {
			if test.Out == nil {
				continue // expected
			}
			t.Errorf("Bind(%v, %v, %v) failed: %v", fn, test.Typedef, test.In, err)
			continue
		}

		if !typex.IsEqualList(actual, test.Out) {
			t.Errorf("Bind(%v, %v, %v) = %v, want %v", fn, test.Typedef, test.In, actual, test.Out)
		}
	}
}
