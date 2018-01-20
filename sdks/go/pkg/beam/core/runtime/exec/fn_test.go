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
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

// TestInvoke verifies the the various forms of input to Invoke are handled correctly.
func TestInvoke(t *testing.T) {
	tests := []struct {
		Fn       interface{}
		Opt      *MainInput
		Args     []interface{}
		Expected interface{}
	}{
		{
			// Void function
			Fn: func() {},
		},
		{
			// Contexts and errors are allowed and handled.
			Fn: func(ctx context.Context) error { return nil },
		},
		{
			// Sum
			Fn:       func(a, b, c int) int { return a + b + c },
			Args:     []interface{}{1, 2, 3},
			Expected: 6,
		},
		{
			// Concat
			Fn:       func(a, b, c string) string { return a + b + c },
			Args:     []interface{}{"a", "b", "c"},
			Expected: "abc",
		},
		{
			// Length (slice type)
			Fn:       func(list []int) (int, error) { return len(list), nil },
			Args:     []interface{}{[]int{1, 2, 3}},
			Expected: 3,
		},
		{
			// Emitter
			Fn:   func(emit func(int)) { emit(1) },
			Args: []interface{}{func(int) {}},
		},
		{
			// Side input
			Fn: func(a int, get func(*int) bool) int {
				var ret int
				if !get(&ret) {
					return a
				}
				return ret
			},
			Args:     []interface{}{1, func(out *int) bool { *out = 2; return true }},
			Expected: 2,
		},
		{
			// Sum as Main
			Fn:       func(a, b, c int) int { return a + b + c },
			Opt:      &MainInput{Key: FullValue{Elm: 1}},
			Args:     []interface{}{2, 3},
			Expected: 6,
		},
		{
			// Sum as Main KV
			Fn:       func(a, b, c int) int { return a + b + c },
			Opt:      &MainInput{Key: FullValue{Elm: 1, Elm2: 2}},
			Args:     []interface{}{3},
			Expected: 6,
		},
		{
			// EventTime
			Fn:       func(ts typex.EventTime, a int) int { return time.Time(ts).Nanosecond() + a },
			Opt:      &MainInput{Key: FullValue{Timestamp: typex.EventTime(time.Unix(0, 2)), Elm: 1}},
			Expected: 3,
		},
	}

	for _, test := range tests {
		fn, err := funcx.New(reflectx.MakeFunc(test.Fn))
		if err != nil {
			t.Fatalf("function not valid: %v", err)
		}

		val, err := Invoke(context.Background(), fn, test.Opt, test.Args...)
		if err != nil {
			t.Fatalf("Invoke(%v,%v) failed: %v", fn.Fn.Name(), test.Args, err)
		}
		if val != nil && val.Elm != test.Expected {
			t.Errorf("Invoke(%v,%v) = %v, want %v", fn.Fn.Name(), test.Args, val.Elm, test.Expected)
		}
	}
}

// Benchmarks

// NOTE(herohde) 12/19/2017: example run on a laptop
//
// BenchmarkDirectCall-4              2000000000	         0.26 ns/op
// BenchmarkIndirectCall-4            1000000000	         2.28 ns/op
// BenchmarkReflectedAndBackCall-4    1000000000	         2.40 ns/op
// BenchmarkReflectCall-4            	10000000	       197    ns/op
// BenchmarkReflectCallNewArgs-4     	10000000	       200    ns/op
// BenchmarkReflectCallReuseArgs-4   	10000000	       196    ns/op
// BenchmarkInvokeCall-4             	 3000000	       452    ns/op
// BenchmarkInvokeCallExtra-4        	 3000000	       419    ns/op
// BenchmarkReflectFnCall-4          	 5000000	       340    ns/op
// BenchmarkInvokeFnCall-4           	 2000000	       604    ns/op
// BenchmarkInvokeFnCallExtra-4      	 3000000	       559    ns/op

func inc(n int) int {
	return n + 1
}

func BenchmarkDirectCall(b *testing.B) {
	n := 0
	for i := 0; i < b.N; i++ {
		n = inc(n)
	}
	b.Log(n)
}

func BenchmarkIndirectCall(b *testing.B) {
	fn := func(int) int { return 0 }
	if b.N > 0 {
		fn = inc // this is always set, but the compiler doesn't know
	}

	n := 0
	for i := 0; i < b.N; i++ {
		n = fn(n)
	}
	b.Log(n)
}

func BenchmarkReflectedAndBackCall(b *testing.B) {
	fn := reflect.ValueOf(inc).Interface().(func(int) int)
	n := 0
	for i := 0; i < b.N; i++ {
		n = fn(n)
	}
	b.Log(n)
}

func BenchmarkReflectCall(b *testing.B) {
	fn := reflect.ValueOf(inc)
	n := reflect.ValueOf(0)
	for i := 0; i < b.N; i++ {
		n = fn.Call([]reflect.Value{n})[0]
	}
	b.Log(n.Interface())
}

func BenchmarkReflectCallNewArgs(b *testing.B) {
	fn := reflect.ValueOf(inc)
	n := reflect.ValueOf(0)
	for i := 0; i < b.N; i++ {
		args := make([]reflect.Value, 1, 1)
		args[0] = n
		n = fn.Call(args)[0]
	}
	b.Log(n.Interface())
}

func BenchmarkReflectCallReuseArgs(b *testing.B) {
	fn := reflect.ValueOf(inc)
	n := reflect.ValueOf(0)
	args := make([]reflect.Value, 1, 1)
	for i := 0; i < b.N; i++ {
		args[0] = n
		n = fn.Call(args)[0]
	}
	b.Log(n.Interface())
}

func BenchmarkInvokeCall(b *testing.B) {
	fn, _ := funcx.New(reflectx.MakeFunc(inc))
	ctx := context.Background()
	n := 0
	for i := 0; i < b.N; i++ {
		ret, _ := Invoke(ctx, fn, &MainInput{Key: FullValue{Elm: n}})
		n = ret.Elm.(int)
	}
	b.Log(n)
}

func BenchmarkInvokeCallExtra(b *testing.B) {
	fn, _ := funcx.New(reflectx.MakeFunc(inc))
	ctx := context.Background()
	n := 0
	for i := 0; i < b.N; i++ {
		ret, _ := Invoke(ctx, fn, nil, n)
		n = ret.Elm.(int)
	}
	b.Log(n)
}

// The below take the additional overhead of MakeFunc.

func incFn(args []reflect.Value) []reflect.Value {
	return []reflect.Value{reflect.ValueOf(args[0].Interface().(int) + 1)}
}

func BenchmarkReflectFnCall(b *testing.B) {
	fn := reflect.MakeFunc(reflect.TypeOf(inc), incFn)
	n := reflect.ValueOf(0)
	for i := 0; i < b.N; i++ {
		n = fn.Call([]reflect.Value{n})[0]
	}
	b.Log(n.Interface())
}

func BenchmarkInvokeFnCall(b *testing.B) {
	fn, _ := funcx.New(reflectx.MakeFunc(reflect.MakeFunc(reflect.TypeOf(inc), incFn).Interface().(func(int) int)))
	ctx := context.Background()
	n := 0
	for i := 0; i < b.N; i++ {
		ret, _ := Invoke(ctx, fn, &MainInput{Key: FullValue{Elm: n}})
		n = ret.Elm.(int)
	}
	b.Log(n)
}

func BenchmarkInvokeFnCallExtra(b *testing.B) {
	fn, _ := funcx.New(reflectx.MakeFunc(reflect.MakeFunc(reflect.TypeOf(inc), incFn).Interface().(func(int) int)))
	ctx := context.Background()
	n := 0
	for i := 0; i < b.N; i++ {
		ret, _ := Invoke(ctx, fn, nil, n)
		n = ret.Elm.(int)
	}
	b.Log(n)
}
