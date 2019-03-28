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
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

type testInt int32

// TestInvoke verifies the the various forms of input to Invoke are handled correctly.
func TestInvoke(t *testing.T) {
	tests := []struct {
		Fn                  interface{}
		Opt                 *MainInput
		Args                []interface{}
		Expected, Expected2 interface{}
		ExpectedTime        typex.EventTime
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
			Fn:       func(ts typex.EventTime, a int) int { return int(ts.Milliseconds()) + a },
			Opt:      &MainInput{Key: FullValue{Elm: 1}},
			Expected: 3,
		},
		{
			// Window
			Fn:       func(w typex.Window, a int) int64 { return w.MaxTimestamp().Milliseconds() },
			Opt:      &MainInput{Key: FullValue{Elm: 1}},
			Expected: mtime.EndOfGlobalWindowTime.Milliseconds(),
		},
		{
			// (Return check) K, V
			Fn:        func(a int) (int64, int) { return int64(a), 2 * a },
			Opt:       &MainInput{Key: FullValue{Elm: 1}},
			Expected:  int64(1),
			Expected2: 2,
		},
		{
			// (Return check)  K, V, Error
			Fn:        func(a int) (int64, int, error) { return int64(a), 2 * a, nil },
			Opt:       &MainInput{Key: FullValue{Elm: 1}},
			Expected:  int64(1),
			Expected2: 2,
		},
		{
			// (Return check) EventTime, K, V
			Fn:           func(a int) (typex.EventTime, int64, int) { return 42, int64(a), 3 * a },
			Opt:          &MainInput{Key: FullValue{Elm: 1}},
			Expected:     int64(1),
			Expected2:    3,
			ExpectedTime: 42,
		},
		{
			// (Return check) EventTime, K, V, Error
			Fn:           func(a int) (typex.EventTime, int64, int, error) { return 47, int64(a), 3 * a, nil },
			Opt:          &MainInput{Key: FullValue{Elm: 1}},
			Expected:     int64(1),
			Expected2:    3,
			ExpectedTime: 47,
		},
		{
			// Check ConvertFn is invoked
			Fn:       func(a string, b []int) (string, int) { return a, len(b) },
			Opt:      &MainInput{Key: FullValue{Elm: "basketball", Elm2: []typex.T{23}}},
			Expected: "basketball", Expected2: 1,
		},
	}

	for i, test := range tests {
		test := test
		t.Run(fmt.Sprintf("%02d:%v", i, reflect.TypeOf(test.Fn)), func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("panic: %v", p)
				}
			}()
			fn, err := funcx.New(reflectx.MakeFunc(test.Fn))
			if err != nil {
				t.Fatalf("function not valid: %v", err)
			}

			ts := mtime.ZeroTimestamp.Add(2 * time.Millisecond)
			if test.ExpectedTime == mtime.ZeroTimestamp {
				test.ExpectedTime = ts
			}

			val, err := Invoke(context.Background(), window.SingleGlobalWindow, ts, fn, test.Opt, test.Args...)
			if err != nil {
				t.Fatalf("Invoke(%v,%v) failed: %v", fn.Fn.Name(), test.Args, err)
			}
			if val != nil && val.Elm != test.Expected {
				t.Errorf("Invoke(%v,%v) = %v, want %v", fn.Fn.Name(), test.Args, val.Elm, test.Expected)
			}
			if val != nil && val.Elm2 != test.Expected2 {
				t.Errorf("Elm2: Invoke(%v,%v) = %v, want %v", fn.Fn.Name(), test.Args, val.Elm2, test.Expected2)
			}
			if val != nil && val.Timestamp != test.ExpectedTime {
				t.Errorf("EventTime: Invoke(%v,%v) = %v, want %v", fn.Fn.Name(), test.Args, val.Timestamp, test.ExpectedTime)
			}
		})
	}
}

// Benchmarks

// Invoke is implemented as a single use of a cached invoker, so a measure of
// correctness is that the cached version takes less time per operation than the
// single use version.
//
// NOTE(lostluck) 2018/07/24: run on a amd64 linux desktop
//
// BenchmarkInvoke/SingleInvoker_Void_function-12         	 5000000	       300 ns/op	     128 B/op	       1 allocs/op
// BenchmarkInvoke/CachedInvoker_Void_function-12         	10000000	       195 ns/op	       0 B/op	       0 allocs/op
// BenchmarkInvoke/SingleInvoker_Contexts_and_errors_are_allowed_and_handled-12         	 2000000	       967 ns/op	     256 B/op	       6 allocs/op
// BenchmarkInvoke/CachedInvoker_Contexts_and_errors_are_allowed_and_handled-12         	 2000000	       854 ns/op	     112 B/op	       4 allocs/op
// BenchmarkInvoke/SingleInvoker_Sum-12                                                 	 2000000	       805 ns/op	     464 B/op	      11 allocs/op
// BenchmarkInvoke/CachedInvoker_Sum-12                                                 	 3000000	       523 ns/op	     224 B/op	       5 allocs/op
// BenchmarkInvoke/SingleInvoker_Concat-12                                              	 2000000	       892 ns/op	     504 B/op	      12 allocs/op
// BenchmarkInvoke/CachedInvoker_Concat-12                                              	 2000000	       594 ns/op	     259 B/op	       6 allocs/op
// BenchmarkInvoke/SingleInvoker_Length_(slice_type)-12                                 	 2000000	       661 ns/op	     384 B/op	       9 allocs/op
// BenchmarkInvoke/CachedInvoker_Length_(slice_type)-12                                 	 3000000	       492 ns/op	     224 B/op	       5 allocs/op
// BenchmarkInvoke/SingleInvoker_Emitter-12                                             	 3000000	       412 ns/op	     184 B/op	       4 allocs/op
// BenchmarkInvoke/CachedInvoker_Emitter-12                                             	 5000000	       268 ns/op	      32 B/op	       1 allocs/op
// BenchmarkInvoke/SingleInvoker_Side_input-12                                          	 2000000	       743 ns/op	     392 B/op	      11 allocs/op
// BenchmarkInvoke/CachedInvoker_Side_input-12                                          	 3000000	       506 ns/op	     200 B/op	       6 allocs/op
// BenchmarkInvoke/SingleInvoker_Sum_as_Main-12                                         	 2000000	       810 ns/op	     464 B/op	      11 allocs/op
// BenchmarkInvoke/CachedInvoker_Sum_as_Main-12                                         	 3000000	       548 ns/op	     224 B/op	       5 allocs/op
// BenchmarkInvoke/SingleInvoker_Sum_as_Main_KV-12                                      	 2000000	       823 ns/op	     464 B/op	      11 allocs/op
// BenchmarkInvoke/CachedInvoker_Sum_as_Main_KV-12                                      	 3000000	       547 ns/op	     224 B/op	       5 allocs/op
// BenchmarkInvoke/SingleInvoker_EventTime-12                                           	 2000000	       711 ns/op	     376 B/op	      10 allocs/op
// BenchmarkInvoke/CachedInvoker_EventTime-12                                           	 3000000	       513 ns/op	     200 B/op	       6 allocs/op
// BenchmarkInvoke/SingleInvoker_Window-12                                              	 1000000	      1023 ns/op	     368 B/op	       9 allocs/op
// BenchmarkInvoke/CachedInvoker_Window-12                                              	 2000000	       838 ns/op	     192 B/op	       5 allocs/op

func BenchmarkInvoke(b *testing.B) {
	tests := []struct {
		Name     string
		Fn       interface{}
		Opt      *MainInput
		Args     []interface{}
		Expected interface{}
	}{
		{
			Name: "Void function",
			Fn:   func() {},
		},
		{
			Name: "Contexts and errors are allowed and handled",
			Fn:   func(ctx context.Context) error { return nil },
		},
		{
			Name:     "Sum",
			Fn:       func(a, b, c int) int { return a + b + c },
			Args:     []interface{}{1, 2, 3},
			Expected: 6,
		},
		{
			Name:     "Concat",
			Fn:       func(a, b, c string) string { return a + b + c },
			Args:     []interface{}{"a", "b", "c"},
			Expected: "abc",
		},
		{
			Name:     "Length (slice type)",
			Fn:       func(list []int) (int, error) { return len(list), nil },
			Args:     []interface{}{[]int{1, 2, 3}},
			Expected: 3,
		},
		{
			Name: "Emitter",
			Fn:   func(emit func(int)) { emit(1) },
			Args: []interface{}{func(int) {}},
		},
		{
			Name: "Side input",
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
			Name:     "Sum as Main",
			Fn:       func(a, b, c int) int { return a + b + c },
			Opt:      &MainInput{Key: FullValue{Elm: 1}},
			Args:     []interface{}{2, 3},
			Expected: 6,
		},
		{
			Name:     "Sum as Main KV",
			Fn:       func(a, b, c int) int { return a + b + c },
			Opt:      &MainInput{Key: FullValue{Elm: 1, Elm2: 2}},
			Args:     []interface{}{3},
			Expected: 6,
		},
		{
			Name:     "EventTime",
			Fn:       func(ts typex.EventTime, a int) int { return int(ts.Milliseconds()) + a },
			Opt:      &MainInput{Key: FullValue{Elm: 1}},
			Expected: 3,
		},
		{
			Name:     "Window",
			Fn:       func(w typex.Window, a int) int64 { return w.MaxTimestamp().Milliseconds() },
			Opt:      &MainInput{Key: FullValue{Elm: 1}},
			Expected: mtime.EndOfGlobalWindowTime.Milliseconds(),
		},
	}

	for _, test := range tests {
		fn, err := funcx.New(reflectx.MakeFunc(test.Fn))
		if err != nil {
			b.Fatalf("function not valid: %v", err)
		}

		ts := mtime.ZeroTimestamp.Add(2 * time.Millisecond)
		b.Run(fmt.Sprintf("SingleInvoker_%s", test.Name), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := Invoke(context.Background(), window.SingleGlobalWindow, ts, fn, test.Opt, test.Args...)
				if err != nil {
					b.Fatalf("Invoke(%v,%v) failed: %v", fn.Fn.Name(), test.Args, err)
				}
			}
		})
		b.Run(fmt.Sprintf("CachedInvoker_%s", test.Name), func(b *testing.B) {
			inv := newInvoker(fn)
			for i := 0; i < b.N; i++ {
				_, err := inv.Invoke(context.Background(), window.SingleGlobalWindow, ts, test.Opt, test.Args...)
				if err != nil {
					b.Fatalf("Invoke(%v,%v) failed: %v", fn.Fn.Name(), test.Args, err)
				}
			}
		})
	}
}

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
		ret, _ := InvokeWithoutEventTime(ctx, fn, &MainInput{Key: FullValue{Elm: n}})
		n = ret.Elm.(int)
	}
	b.Log(n)
}

func BenchmarkInvokeCallExtra(b *testing.B) {
	fn, _ := funcx.New(reflectx.MakeFunc(inc))
	ctx := context.Background()
	n := 0
	for i := 0; i < b.N; i++ {
		ret, _ := InvokeWithoutEventTime(ctx, fn, nil, n)
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
		ret, _ := InvokeWithoutEventTime(ctx, fn, &MainInput{Key: FullValue{Elm: n}})
		n = ret.Elm.(int)
	}
	b.Log(n)
}

func BenchmarkInvokeFnCallExtra(b *testing.B) {
	fn, _ := funcx.New(reflectx.MakeFunc(reflect.MakeFunc(reflect.TypeOf(inc), incFn).Interface().(func(int) int)))
	ctx := context.Background()
	n := 0
	for i := 0; i < b.N; i++ {
		ret, _ := InvokeWithoutEventTime(ctx, fn, nil, n)
		n = ret.Elm.(int)
	}
	b.Log(n)
}

// Foo is a struct with a method for measuring method invocation
// overhead for StructuralDoFns.
type Foo struct {
	A int
}

// WhatsA is a method for measuring a baseline of structural dofn overhead.
func (f *Foo) WhatsA(b int) int {
	return f.A + b
}

// WhatsB is a comparable direct function for baseline comparison.
func WhatsB(b int) int {
	return 32 + b
}

// Expclicit Receiver Type assertion shim.
type callerFooRInt struct {
	fn func(*Foo, int) int
}

func funcMakerFooRInt(fn interface{}) reflectx.Func {
	f := fn.(func(*Foo, int) int)
	return &callerFooRInt{fn: f}
}

func (c *callerFooRInt) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerFooRInt) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerFooRInt) Call(args []interface{}) []interface{} {
	a := c.fn(args[0].(*Foo), args[1].(int))
	return []interface{}{a}
}

// To satisfy reflectx.Func2x1
func (c *callerFooRInt) Call2x1(a1, a2 interface{}) interface{} {
	return c.fn(a1.(*Foo), a2.(int))
}

// Implicit Receiver type assertion shim.
type callerInt struct {
	fn func(int) int
}

func funcMakerInt(fn interface{}) reflectx.Func {
	f := fn.(func(int) int)
	return &callerInt{fn: f}
}

func (c *callerInt) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerInt) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerInt) Call(args []interface{}) []interface{} {
	a := c.fn(args[0].(int))
	return []interface{}{a}
}

func (c *callerInt) Call1x1(a0 interface{}) interface{} {
	return c.fn(a0.(int))
}

// BenchmarkMethodCalls measures the overhead of different ways of
// "generically" invoking methods.
//
// This benchmark invokes methods along several different axes
// * Implicit or Explicit method Receiver
// * Pre-wrapped values and pre-allocated slices.
// * Invocations along the following ways
//   * Indirect via extracting from a reflect.Value.Interface()
//   * Reflect Package (reflect.Value.Call())
//   * Beam's reflectx.Func, and reflectx.FuncNxM interfaces
//       * Beam's default reflection based reflectx.Func shim
//       * A Type assertion specialized reflectx.Func shim
//
// The Implicit or Explicit method receiver difference exists because
// Go's reflect package treats the two cases different, and there are
// performance implications this benchmark captures. Implicit adds a
// fixed amount of overhead perf invocation giving a performance penalty
// to Structural DoFns.
//
// PreAlocating slices and values serves as a comparison point for how much
// overhead not doing these things costs. In particular in wrapping/unwrapping
// values with reflect.ValueOf and extracting them with reflect.Value's
// Interface() method.
func BenchmarkMethodCalls(b *testing.B) {
	f := &Foo{A: 3}
	var gi interface{}
	g := &Foo{A: 42}
	gi = g
	gV := reflect.ValueOf(g)
	fV := reflect.ValueOf(f)

	indirectFunc := reflect.ValueOf(WhatsB).Interface().(func(int) int)

	// Implicit Receivers
	impRF := fV.Method(0)
	impRFi := impRF.Interface().(func(int) int)
	impRxF := reflectx.MakeFunc(impRFi)
	impRx1x1F := reflectx.ToFunc1x1(impRxF)
	impRShimF := funcMakerInt(impRFi)             // as if this shim were registered
	impRShim1x1F := reflectx.ToFunc1x1(impRShimF) // would be MakeFunc1x1 if registered

	// Explicit Receivers
	expRF := fV.Type().Method(0).Func
	expRFi := expRF.Interface().(func(*Foo, int) int)

	expRxF := reflectx.MakeFunc(expRFi)
	expRx2c1F := reflectx.ToFunc2x1(expRxF)
	expRShimF := funcMakerFooRInt(expRFi)         // as if this shim were registered
	expRShim2x1F := reflectx.ToFunc2x1(expRShimF) // would be MakeFunc2x1 if registered

	// Closured Receivers
	wrappedWhatsA := func(a int) int { return f.WhatsA(a) }
	clsrRF := reflect.ValueOf(wrappedWhatsA)
	clsrRFi := clsrRF.Interface().(func(int) int)
	clsrRxF := reflectx.MakeFunc(clsrRFi)
	clsrRx1x1F := reflectx.ToFunc1x1(clsrRxF)
	clsrRShimF := funcMakerInt(clsrRFi)             // as if this shim were registered
	clsrRShim1x1F := reflectx.ToFunc1x1(clsrRShimF) // would be MakeFunc1x1 if registered

	// Parameters
	var a int
	var ai interface{} = a
	aV := reflect.ValueOf(a)
	rvSlice := []reflect.Value{aV}
	grvSlice := []reflect.Value{gV, aV}
	efaceSlice := []interface{}{a}
	gEfaceSlice := []interface{}{g, a}

	tests := []struct {
		name string
		fn   func()
	}{
		{"DirectMethod", func() { a = g.WhatsA(a) }},     // Baseline as low as we can go.
		{"DirectFunc", func() { a = WhatsB(a) }},         // For comparison purposes
		{"IndirectFunc", func() { a = indirectFunc(a) }}, // For comparison purposes

		// Implicits
		{"IndirectImplicit", func() { a = impRFi(a) }},             // Measures the indirection through reflect.Value cost.
		{"TypeAssertedImplicit", func() { ai = impRFi(ai.(int)) }}, // Measures the type assertion cost over the above.

		{"ReflectCallImplicit", func() { a = impRF.Call([]reflect.Value{reflect.ValueOf(a)})[0].Interface().(int) }},
		{"ReflectCallImplicit-NoWrap", func() { a = impRF.Call([]reflect.Value{aV})[0].Interface().(int) }},
		{"ReflectCallImplicit-NoReallocSlice", func() { a = impRF.Call(rvSlice)[0].Interface().(int) }},

		{"ReflectXCallImplicit", func() { a = impRxF.Call([]interface{}{a})[0].(int) }},
		{"ReflectXCallImplicit-NoReallocSlice", func() { a = impRxF.Call(efaceSlice)[0].(int) }},
		{"ReflectXCall1x1Implicit", func() { a = impRx1x1F.Call1x1(a).(int) }}, // Measures the default shimfunc overhead.

		{"ShimedCallImplicit", func() { a = impRShimF.Call([]interface{}{a})[0].(int) }},          // What we're currently using for invoking methods
		{"ShimedCallImplicit-NoReallocSlice", func() { a = impRShimF.Call(efaceSlice)[0].(int) }}, // Closer to what we're using now.
		{"ShimedCall1x1Implicit", func() { a = impRShim1x1F.Call1x1(a).(int) }},

		// Explicit
		{"IndirectExplicit", func() { a = expRFi(g, a) }},                     // Measures the indirection through reflect.Value cost.
		{"TypeAssertedExplicit", func() { ai = expRFi(gi.(*Foo), ai.(int)) }}, // Measures the type assertion cost over the above.

		{"ReflectCallExplicit", func() { a = expRF.Call([]reflect.Value{reflect.ValueOf(g), reflect.ValueOf(a)})[0].Interface().(int) }},
		{"ReflectCallExplicit-NoWrap", func() { a = expRF.Call([]reflect.Value{gV, aV})[0].Interface().(int) }},
		{"ReflectCallExplicit-NoReallocSlice", func() { a = expRF.Call(grvSlice)[0].Interface().(int) }},

		{"ReflectXCallExplicit", func() { a = expRxF.Call([]interface{}{g, a})[0].(int) }},
		{"ReflectXCallExplicit-NoReallocSlice", func() { a = expRxF.Call(gEfaceSlice)[0].(int) }},
		{"ReflectXCall2x1Explicit", func() { a = expRx2c1F.Call2x1(g, a).(int) }},

		{"ShimedCallExplicit", func() { a = expRShimF.Call([]interface{}{g, a})[0].(int) }},
		{"ShimedCallExplicit-NoReallocSlice", func() { a = expRShimF.Call(gEfaceSlice)[0].(int) }},
		{"ShimedCall2x1Explicit", func() { a = expRShim2x1F.Call2x1(g, a).(int) }},

		// Closured
		{"IndirectClosured", func() { a = clsrRFi(a) }},             // Measures the indirection through reflect.Value cost.
		{"TypeAssertedClosured", func() { ai = clsrRFi(ai.(int)) }}, // Measures the type assertion cost over the above.

		{"ReflectCallClosured", func() { a = clsrRF.Call([]reflect.Value{reflect.ValueOf(a)})[0].Interface().(int) }},
		{"ReflectCallClosured-NoWrap", func() { a = clsrRF.Call([]reflect.Value{aV})[0].Interface().(int) }},
		{"ReflectCallClosured-NoReallocSlice", func() { a = clsrRF.Call(rvSlice)[0].Interface().(int) }},

		{"ReflectXCallClosured", func() { a = clsrRxF.Call([]interface{}{a})[0].(int) }},
		{"ReflectXCallClosured-NoReallocSlice", func() { a = clsrRxF.Call(efaceSlice)[0].(int) }},
		{"ReflectXCall1x1Closured", func() { a = clsrRx1x1F.Call1x1(a).(int) }}, // Measures the default shimfunc overhead.

		{"ShimedCallClosured", func() { a = clsrRShimF.Call([]interface{}{a})[0].(int) }},          // What we're currently using for invoking methods
		{"ShimedCallClosured-NoReallocSlice", func() { a = clsrRShimF.Call(efaceSlice)[0].(int) }}, // Closer to what we're using now.
		{"ShimedCall1x1Closured", func() { a = clsrRShim1x1F.Call1x1(a).(int) }},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				test.fn()
			}
		})
	}
	b.Log(a)
}

/*
@lostluck 2018/11/29 on a Intel(R) Core(TM) i7-7Y75 CPU @ 1.30GHz Pixelbook.
The actual times will vary per machine, but the relative differences are unlikely to change
between machines.

*** Standard candles **
BenchmarkMethodCalls/DirectMethod-4             1000000000               2.00 ns/op
BenchmarkMethodCalls/DirectFunc-4               2000000000               1.87 ns/op
BenchmarkMethodCalls/IndirectFunc-4             300000000                4.68 ns/op

*** Implicit receiver variants **
BenchmarkMethodCalls/IndirectImplicit-4         10000000               187 ns/op
BenchmarkMethodCalls/TypeAssertedImplicit-4     10000000               207 ns/op
BenchmarkMethodCalls/ReflectCallImplicit-4       5000000               362 ns/op
BenchmarkMethodCalls/ReflectCallImplicit-NoWrap-4                5000000               350 ns/op
BenchmarkMethodCalls/ReflectCallImplicit-NoReallocSlice-4        5000000               365 ns/op
BenchmarkMethodCalls/ReflectXCallImplicit-4                      2000000               874 ns/op
BenchmarkMethodCalls/ReflectXCallImplicit-NoReallocSlice-4               2000000              1227 ns/op **Default**
BenchmarkMethodCalls/ReflectXCall1x1Implicit-4                           1000000              1184 ns/op
BenchmarkMethodCalls/ShimedCallImplicit-4                                2000000               647 ns/op
BenchmarkMethodCalls/ShimedCallImplicit-NoReallocSlice-4                 3000000               589 ns/op
BenchmarkMethodCalls/ShimedCall1x1Implicit-4                             3000000               446 ns/op

*** Explicit receiver variants ***
BenchmarkMethodCalls/IndirectExplicit-4                                 200000000                7.64 ns/op
BenchmarkMethodCalls/TypeAssertedExplicit-4                             50000000                26.9 ns/op
BenchmarkMethodCalls/ReflectCallExplicit-4                               3000000               430 ns/op
BenchmarkMethodCalls/ReflectCallExplicit-NoWrap-4                        5000000               394 ns/op
BenchmarkMethodCalls/ReflectCallExplicit-NoReallocSlice-4                3000000               375 ns/op
BenchmarkMethodCalls/ReflectXCallExplicit-4                              2000000               621 ns/op
BenchmarkMethodCalls/ReflectXCallExplicit-NoReallocSlice-4               3000000               552 ns/op
BenchmarkMethodCalls/ReflectXCall2x1Explicit-4                           2000000               839 ns/op
BenchmarkMethodCalls/ShimedCallExplicit-4                                5000000               208 ns/op
BenchmarkMethodCalls/ShimedCallExplicit-NoReallocSlice-4                20000000                70.1 ns/op
BenchmarkMethodCalls/ShimedCall2x1Explicit-4                            30000000                48.9 ns/op

*** Closured direct invocation variants ***
BenchmarkMethodCalls/IndirectClosured-4                                 300000000                4.93 ns/op
BenchmarkMethodCalls/TypeAssertedClosured-4                             100000000               25.7 ns/op
BenchmarkMethodCalls/ReflectCallClosured-4                               5000000               318 ns/op
BenchmarkMethodCalls/ReflectCallClosured-NoWrap-4                        5000000               269 ns/op
BenchmarkMethodCalls/ReflectCallClosured-NoReallocSlice-4                5000000               266 ns/op
BenchmarkMethodCalls/ReflectXCallClosured-4                              3000000               440 ns/op
BenchmarkMethodCalls/ReflectXCallClosured-NoReallocSlice-4               5000000               377 ns/op
BenchmarkMethodCalls/ReflectXCall1x1Closured-4                           3000000               460 ns/op
BenchmarkMethodCalls/ShimedCallClosured-4                               20000000               113 ns/op
BenchmarkMethodCalls/ShimedCallClosured-NoReallocSlice-4                20000000                61.5 ns/op **With specialized shims**
BenchmarkMethodCalls/ShimedCall1x1Closured-4                            30000000                45.5 ns/op **Arity specialized re-work of the invoker**
*/
