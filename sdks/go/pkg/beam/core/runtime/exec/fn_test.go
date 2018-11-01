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
	}

	for _, test := range tests {
		fn, err := funcx.New(reflectx.MakeFunc(test.Fn))
		if err != nil {
			t.Fatalf("function not valid: %v", err)
		}

		ts := mtime.ZeroTimestamp.Add(2 * time.Millisecond)

		val, err := Invoke(context.Background(), window.SingleGlobalWindow, ts, fn, test.Opt, test.Args...)
		if err != nil {
			t.Fatalf("Invoke(%v,%v) failed: %v", fn.Fn.Name(), test.Args, err)
		}
		if val != nil && val.Elm != test.Expected {
			t.Errorf("Invoke(%v,%v) = %v, want %v", fn.Fn.Name(), test.Args, val.Elm, test.Expected)
		}
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

	nrF := fV.Method(0)
	nrFi := nrF.Interface().(func(int) int)
	rxnrF := reflectx.MakeFunc(nrFi)
	rx0x1nrF := reflectx.ToFunc1x1(rxnrF)
	shimnrF := funcMakerInt(nrFi)             // as if this shim were registered
	shim0x1nrF := reflectx.ToFunc1x1(shimnrF) // would be MakeFunc0x1 if registered

	wrF := fV.Type().Method(0).Func
	wrFi := wrF.Interface().(func(*Foo, int) int)

	rxF := reflectx.MakeFunc(wrFi)
	rx1x1F := reflectx.ToFunc2x1(rxF)
	shimF := funcMakerFooRInt(wrFi)       // as if this shim were registered
	shim1x1F := reflectx.ToFunc2x1(shimF) // would be MakeFunc1x1 if registered

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
		{"DirectMethod", func() { a = g.WhatsA(a) }}, // Baseline as low as we can go.
		{"DirectFunc", func() { a = WhatsB(a) }},     // For comparison purposes

		{"IndirectFunc", func() { a = indirectFunc(a) }},         // For comparison purposes
		{"IndirectImplicit", func() { a = nrFi(a) }},             // Measures the indirection through reflect.Value cost.
		{"TypeAssertedImplicit", func() { ai = nrFi(ai.(int)) }}, // Measures the type assertion cost over the above.

		{"ReflectCallImplicit", func() { a = nrF.Call([]reflect.Value{reflect.ValueOf(a)})[0].Interface().(int) }},
		{"ReflectCallImplicit-NoWrap", func() { a = nrF.Call([]reflect.Value{aV})[0].Interface().(int) }},
		{"ReflectCallImplicit-NoReallocSlice", func() { a = nrF.Call(rvSlice)[0].Interface().(int) }},

		{"ReflectXCallImplicit", func() { a = rxnrF.Call([]interface{}{a})[0].(int) }},
		{"ReflectXCallImplicit-NoReallocSlice", func() { a = rxnrF.Call(efaceSlice)[0].(int) }},
		{"ReflectXCall1x1Implicit", func() { a = rx0x1nrF.Call1x1(a).(int) }}, // Measures the default shimfunc overhead.

		{"ShimedCallImplicit", func() { a = shimnrF.Call([]interface{}{a})[0].(int) }},          // What we're currently using for invoking methods
		{"ShimedCallImplicit-NoReallocSlice", func() { a = shimnrF.Call(efaceSlice)[0].(int) }}, // Closer to what we're using now.
		{"ShimedCall1x1Implicit", func() { a = shim0x1nrF.Call1x1(a).(int) }},

		{"IndirectExplicit", func() { a = wrFi(g, a) }},                     // Measures the indirection through reflect.Value cost.
		{"TypeAssertedExplicit", func() { ai = wrFi(gi.(*Foo), ai.(int)) }}, // Measures the type assertion cost over the above.

		{"ReflectCallExplicit", func() { a = wrF.Call([]reflect.Value{reflect.ValueOf(g), reflect.ValueOf(a)})[0].Interface().(int) }},
		{"ReflectCallExplicit-NoWrap", func() { a = wrF.Call([]reflect.Value{gV, aV})[0].Interface().(int) }},
		{"ReflectCallExplicit-NoReallocSlice", func() { a = wrF.Call(grvSlice)[0].Interface().(int) }},

		{"ReflectXCallExplicit", func() { a = rxF.Call([]interface{}{g, a})[0].(int) }},
		{"ReflectXCallExplicit-NoReallocSlice", func() { a = rxF.Call(gEfaceSlice)[0].(int) }},
		{"ReflectXCall2x1Explicit", func() { a = rx1x1F.Call2x1(g, a).(int) }},

		{"ShimedCallExplicit", func() { a = shimF.Call([]interface{}{g, a})[0].(int) }},
		{"ShimedCallExplicit-NoReallocSlice", func() { a = shimF.Call(gEfaceSlice)[0].(int) }},
		{"ShimedCall2x1Explicit", func() { a = shim1x1F.Call2x1(g, a).(int) }},
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
@lostluck 2018/10/30 on a desktop machine.

BenchmarkMethodCalls/DirectMethod-12                         1000000000	         2.02 ns/op
BenchmarkMethodCalls/DirectFunc-12                           2000000000	         1.81 ns/op
BenchmarkMethodCalls/IndirectFunc-12         	              300000000	         4.66 ns/op
BenchmarkMethodCalls/IndirectImplicit-12                       10000000	       185 ns/op
BenchmarkMethodCalls/TypeAssertedImplicit-12                   10000000	       228 ns/op
BenchmarkMethodCalls/ReflectCallImplicit-12                     3000000	       479 ns/op
BenchmarkMethodCalls/ReflectCallImplicit-NoWrap-12              3000000	       451 ns/op
BenchmarkMethodCalls/ReflectCallImplicit-NoReallocSlice-12      3000000	       424 ns/op
BenchmarkMethodCalls/ReflectXCallImplicit-12                    2000000	       756 ns/op
BenchmarkMethodCalls/ReflectXCallImplicit-NoReallocSlice-12		2000000	       662 ns/op **Default**
BenchmarkMethodCalls/ReflectXCall1x1Implicit-12                 2000000	       762 ns/op
BenchmarkMethodCalls/ShimedCallImplicit-12                      5000000	       374 ns/op
BenchmarkMethodCalls/ShimedCallImplicit-NoReallocSlice-12       5000000	       289 ns/op **With specialized shims**
BenchmarkMethodCalls/ShimedCall1x1Implicit-12                   5000000	       249 ns/op **Arity specialized re-work of the invoker**

** Everything below requires an overhaul of structural DoFn invocation code, and regeneration of all included shims. **
BenchmarkMethodCalls/IndirectExplicit-12                      300000000	         4.81 ns/op
BenchmarkMethodCalls/TypeAssertedExplicit-12                   50000000	        35.4 ns/op
BenchmarkMethodCalls/ReflectCallExplicit-12                     3000000	       434 ns/op
BenchmarkMethodCalls/ReflectCallExplicit-NoWrap-12              5000000	       397 ns/op
BenchmarkMethodCalls/ReflectCallExplicit-NoReallocSlice-12      5000000	       390 ns/op
BenchmarkMethodCalls/ReflectXCallExplicit-12                    2000000	       755 ns/op
BenchmarkMethodCalls/ReflectXCallExplicit-NoReallocSlice-12     2000000	       601 ns/op
BenchmarkMethodCalls/ReflectXCall2x1Explicit-12                 2000000	       735 ns/op
BenchmarkMethodCalls/ShimedCallExplicit-12                     10000000	       198 ns/op
BenchmarkMethodCalls/ShimedCallExplicit-NoReallocSlice-12      20000000	        93.5 ns/op
BenchmarkMethodCalls/ShimedCall2x1Explicit-12                  20000000	        68.3 ns/op  **Best we could do**
*/
