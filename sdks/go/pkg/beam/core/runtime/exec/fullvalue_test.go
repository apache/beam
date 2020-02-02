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

package exec

import (
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
)

func makeInput(vs ...interface{}) []MainInput {
	var ret []MainInput
	for _, v := range makeValues(vs...) {
		ret = append(ret, MainInput{Key: v})
	}
	return ret
}

func makeValues(vs ...interface{}) []FullValue {
	var ret []FullValue
	for _, v := range vs {
		ret = append(ret, FullValue{
			Windows:   window.SingleGlobalWindow,
			Timestamp: mtime.ZeroTimestamp,
			Elm:       v,
		})
	}
	return ret
}

func makeValuesNoWindowOrTime(vs ...interface{}) []FullValue {
	var ret []FullValue
	for _, v := range vs {
		ret = append(ret, FullValue{
			Elm: v,
		})
	}
	return ret
}

// makeKVValues returns a list of KV<K,V> inputs as a list of main inputs.
func makeKVInput(key interface{}, vs ...interface{}) []MainInput {
	var ret []MainInput
	for _, v := range makeKVValues(key, vs...) {
		ret = append(ret, MainInput{Key: v})
	}
	return ret
}

// makeKVValues returns a list of KV<K,V> inputs.
func makeKVValues(key interface{}, vs ...interface{}) []FullValue {
	var ret []FullValue
	for _, v := range vs {
		k := FullValue{
			Windows:   window.SingleGlobalWindow,
			Timestamp: mtime.ZeroTimestamp,
			Elm:       key,
			Elm2:      v,
		}
		ret = append(ret, k)
	}
	return ret
}

// makeKeyedInput returns a CoGBK<K, V> where the list of values are a stream
// in a single main input.
func makeKeyedInput(key interface{}, vs ...interface{}) []MainInput {
	k := FullValue{
		Windows:   window.SingleGlobalWindow,
		Timestamp: mtime.ZeroTimestamp,
		Elm:       key,
	}
	return []MainInput{{
		Key:    k,
		Values: []ReStream{&FixedReStream{Buf: makeValues(vs...)}},
	}}
}

func makeKV(k, v interface{}) []FullValue {
	return []FullValue{{
		Windows:   window.SingleGlobalWindow,
		Timestamp: mtime.ZeroTimestamp,
		Elm:       k,
		Elm2:      v,
	}}
}

func extractValues(vs ...FullValue) []interface{} {
	var ret []interface{}
	for _, v := range vs {
		ret = append(ret, v.Elm)
	}
	return ret
}

func extractKeyedValues(vs ...FullValue) []interface{} {
	var ret []interface{}
	for _, v := range vs {
		ret = append(ret, v.Elm2)
	}
	return ret
}

func equalList(a, b []FullValue) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if !equal(v, b[i]) {
			return false
		}
	}
	return true
}

func equal(a, b FullValue) bool {
	if a.Timestamp != b.Timestamp {
		return false
	}
	if (a.Elm == nil) != (b.Elm == nil) {
		return false
	}
	if (a.Elm2 == nil) != (b.Elm2 == nil) {
		return false
	}

	if a.Elm != nil {
		if !reflect.DeepEqual(a.Elm, b.Elm) {
			return false
		}
	}
	if a.Elm2 != nil {
		if !reflect.DeepEqual(a.Elm2, b.Elm2) {
			return false
		}
	}
	if len(a.Windows) != len(b.Windows) {
		return false
	}
	for i, w := range a.Windows {
		if !w.Equals(b.Windows[i]) {
			return false
		}
	}
	return true
}

type testStruct struct {
	a int
}

func (*testStruct) M() {}

// Conversion tests.
func TestConvert(t *testing.T) {
	tests := []struct {
		name    string
		to      reflect.Type
		v, want interface{}
	}{
		{
			name: "int_to_int",
			to:   reflectx.Int,
			v:    typex.T(42),
			want: int(42),
		},
		{
			name: "typexT_to_int",
			to:   reflectx.Int,
			v:    typex.T(42),
			want: int(42),
		},
		{
			name: "[]typexT_to_[]int",
			to:   reflect.TypeOf([]int{}),
			v:    []typex.T{1, 2, 3},
			want: []int{1, 2, 3},
		},
		{
			name: "[]typexT_to_typexX",
			to:   typex.XType,
			v:    []typex.T{1, 2, 3},
			want: []int{1, 2, 3},
		},
		{
			name: "empty_[]typexT_to_typexX",
			to:   typex.XType,
			v:    []typex.T{},
			want: []typex.T{},
		},
		{
			name: "nil_[]typexT_to_typexX",
			to:   typex.XType,
			v:    []typex.T(nil),
			want: []typex.T(nil),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			if got := Convert(test.v, test.to); !reflect.DeepEqual(got, test.want) {
				t.Errorf("Convert(%v,%v) = %v,  want %v", test.v, test.to, got, test.want)
			}
		})
		t.Run("Fn_"+test.name, func(t *testing.T) {
			fn := ConvertFn(reflect.TypeOf(test.v), test.to)
			if got := fn(test.v); !reflect.DeepEqual(got, test.want) {
				t.Errorf("ConvertFn(%T, %v)(%v) = %v,  want %v", test.v, test.to, test.v, got, test.want)
			}
		})
	}
}
