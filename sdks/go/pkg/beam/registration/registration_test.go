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

package registration

import (
	"context"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

func TestRegister_CompleteDoFn_WrapsStruct(t *testing.T) {
	doFn := &CompleteDoFn2x1{}
	DoFn2x1[string, func(int), int](doFn)

	m, ok := reflectx.WrapMethods(&CompleteDoFn2x1{})
	if !ok {
		t.Fatalf("reflectx.WrapMethods(CompleteDoFn2x1{}), no registered entry found")
	}
	pe, ok := m["ProcessElement"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(CompleteDoFn2x1{}), no registered entry found for ProcessElement")
	}
	val := 0
	emit := func(a0 int) {
		val = a0
	}
	if got, want := pe.Call([]interface{}{"hello", emit})[0].(int), 5; got != want {
		t.Errorf("Wrapped ProcessElement call: got %v, want %v", got, want)
	}
	if got, want := val, 5; got != want {
		t.Errorf("Wrapped ProcessElement call emit result: got %v, want %v", got, want)
	}
	sb, ok := m["StartBundle"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(CompleteDoFn2x1{}), no registered entry found for StartBundle")
	}
	val = 0
	emit = func(a0 int) {
		val = a0
	}
	sb.Call([]interface{}{context.Background(), emit})
	if got, want := val, 2; got != want {
		t.Errorf("Wrapped StartBundle call emit result: got %v, want %v", got, want)
	}
	fb, ok := m["FinishBundle"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(CompleteDoFn2x1{}), no registered entry found for FinishBundle")
	}
	val = 0
	emit = func(a0 int) {
		val = a0
	}
	if got := fb.Call([]interface{}{context.Background(), emit})[0]; got != nil {
		t.Errorf("Wrapped FinishBundle call: got %v, want nil", got)
	}
	if got, want := val, 2; got != want {
		t.Errorf("Wrapped FinishBundle call emit result: got %v, want %v", got, want)
	}
	su, ok := m["Setup"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(CompleteDoFn2x1{}), no registered entry found for Setup")
	}
	if got := su.Call([]interface{}{context.Background()})[0]; got != nil {
		t.Errorf("Wrapped FinishBundle call: got %v, want nil", got)
	}
	td, ok := m["Teardown"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(CompleteDoFn2x1{}), no registered entry found for Teardown")
	}
	if got := td.Call([]interface{}{})[0]; got != nil {
		t.Errorf("Wrapped Teardown call: got %v, want nil", got)
	}
}

func TestRegister_PartialDoFn_WrapsStruct(t *testing.T) {
	doFn := &PartialDoFn1x0{}
	DoFn1x0[func(int)](doFn)

	m, ok := reflectx.WrapMethods(&PartialDoFn1x0{})
	if !ok {
		t.Fatalf("reflectx.WrapMethods(PartialDoFn1x0{}), no registered entry found")
	}
	pe, ok := m["ProcessElement"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(PartialDoFn1x0{}), no registered entry found for ProcessElement")
	}
	val := 0
	emit := func(a0 int) {
		val = a0
	}
	pe.Call([]interface{}{emit})
	if got, want := val, 5; got != want {
		t.Errorf("Wrapped ProcessElement call emit result: got %v, want %v", got, want)
	}
	sb, ok := m["StartBundle"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(PartialDoFn1x0{}), no registered entry found for StartBundle")
	}
	val = 0
	emit = func(a0 int) {
		val = a0
	}
	sb.Call([]interface{}{context.Background(), emit})
	if got, want := val, 2; got != want {
		t.Errorf("Wrapped StartBundle call emit result: got %v, want %v", got, want)
	}
}

func TestRegister_ProcessElementDoFn_WrapsStruct(t *testing.T) {
	doFn := &ProcessElementDoFn0x1{}
	DoFn0x1[int](doFn)

	m, ok := reflectx.WrapMethods(&ProcessElementDoFn0x1{})
	if !ok {
		t.Fatalf("reflectx.WrapMethods(ProcessElementDoFn0x1{}), no registered entry found")
	}
	pe, ok := m["ProcessElement"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(ProcessElementDoFn0x1{}), no registered entry found for ProcessElement")
	}
	if got, want := pe.Call([]interface{}{})[0].(int), 5; got != want {
		t.Errorf("Wrapped ProcessElement call: got %v, want %v", got, want)
	}
}

func TestRegister_RegistersTypes(t *testing.T) {
	doFn := &CustomTypeDoFn1x1{}
	DoFn1x1[CustomType, CustomType2](doFn)
	// Need to call FromType so that the registry will reconcile its registrations
	schema.FromType(reflect.TypeOf(doFn).Elem())
	if !schema.Registered(reflect.TypeOf(doFn).Elem()) {
		t.Errorf("schema.Registered(reflect.TypeOf(CustomTypeDoFn1x1{})) = false, want true")
	}
	if !schema.Registered(reflect.TypeOf(CustomType{})) {
		t.Errorf("schema.Registered(reflect.TypeOf(CustomType{})) = false, want true")
	}
	if !schema.Registered(reflect.TypeOf(CustomType2{})) {
		t.Errorf("schema.Registered(reflect.TypeOf(CustomType{})) = false, want true")
	}
}

func TestEmitter1(t *testing.T) {
	Emitter1[int]()
	if !exec.IsEmitterRegistered(reflect.TypeOf((*func(int))(nil)).Elem()) {
		t.Fatalf("exec.IsEmitterRegistered(reflect.TypeOf((*func(int))(nil)).Elem()) = false, want true")
	}
}

func TestEmitter2(t *testing.T) {
	Emitter2[int, string]()
	if !exec.IsEmitterRegistered(reflect.TypeOf((*func(int, string))(nil)).Elem()) {
		t.Fatalf("exec.IsEmitterRegistered(reflect.TypeOf((*func(int, string))(nil)).Elem()) = false, want true")
	}
}

func TestEmitter2_WithTimestamp(t *testing.T) {
	Emitter2[typex.EventTime, string]()
	if !exec.IsEmitterRegistered(reflect.TypeOf((*func(typex.EventTime, string))(nil)).Elem()) {
		t.Fatalf("exec.IsEmitterRegistered(reflect.TypeOf((*func(typex.EventTime, string))(nil)).Elem()) = false, want true")
	}
}

func TestEmitter3(t *testing.T) {
	Emitter3[typex.EventTime, int, string]()
	if !exec.IsEmitterRegistered(reflect.TypeOf((*func(typex.EventTime, int, string))(nil)).Elem()) {
		t.Fatalf("exec.IsEmitterRegistered(reflect.TypeOf((*func(typex.EventTime, int, string))(nil)).Elem()) = false, want true")
	}
}

func TestEmit1(t *testing.T) {
	e := &emit1[int]{n: &elementProcessor{}}
	e.Init(context.Background(), []typex.Window{}, mtime.ZeroTimestamp)
	fn := e.Value().(func(int))
	fn(3)
	if got, want := e.n.(*elementProcessor).inFV.Elm, 3; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Elm=%v, want %v", got, want)
	}
	if got := e.n.(*elementProcessor).inFV.Elm2; got != nil {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Elm2=%v, want nil", got)
	}
	if got, want := e.n.(*elementProcessor).inFV.Timestamp, mtime.ZeroTimestamp; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Timestamp=%v, want %v", got, want)
	}
}

func TestEmit2(t *testing.T) {
	e := &emit2[int, string]{n: &elementProcessor{}}
	e.Init(context.Background(), []typex.Window{}, mtime.ZeroTimestamp)
	fn := e.Value().(func(int, string))
	fn(3, "hello")
	if got, want := e.n.(*elementProcessor).inFV.Elm, 3; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Elm=%v, want %v", got, want)
	}
	if got, want := e.n.(*elementProcessor).inFV.Elm2, "hello"; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Elm2=%v, want %v", got, want)
	}
	if got, want := e.n.(*elementProcessor).inFV.Timestamp, mtime.ZeroTimestamp; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Timestamp=%v, want %v", got, want)
	}
}

func TestEmit1WithTimestamp(t *testing.T) {
	e := &emit1WithTimestamp[int]{n: &elementProcessor{}}
	e.Init(context.Background(), []typex.Window{}, mtime.ZeroTimestamp)
	fn := e.Value().(func(typex.EventTime, int))
	fn(mtime.MaxTimestamp, 3)
	if got, want := e.n.(*elementProcessor).inFV.Elm, 3; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Elm=%v, want %v", got, want)
	}
	if got := e.n.(*elementProcessor).inFV.Elm2; got != nil {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Elm2=%v, want nil", got)
	}
	if got, want := e.n.(*elementProcessor).inFV.Timestamp, mtime.MaxTimestamp; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Timestamp=%v, want %v", got, want)
	}
}

func TestEmit2WithTimestamp(t *testing.T) {
	e := &emit2WithTimestamp[int, string]{n: &elementProcessor{}}
	e.Init(context.Background(), []typex.Window{}, mtime.ZeroTimestamp)
	fn := e.Value().(func(typex.EventTime, int, string))
	fn(mtime.MaxTimestamp, 3, "hello")
	if got, want := e.n.(*elementProcessor).inFV.Elm, 3; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Elm=%v, want %v", got, want)
	}
	if got, want := e.n.(*elementProcessor).inFV.Elm2, "hello"; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Elm2=%v, want %v", got, want)
	}
	if got, want := e.n.(*elementProcessor).inFV.Timestamp, mtime.MaxTimestamp; got != want {
		t.Errorf("e.Value.(func(int))(3).n.inFV.Timestamp=%v, want %v", got, want)
	}
}

func TestIter1(t *testing.T) {
	Iter1[int]()
	if !exec.IsInputRegistered(reflect.TypeOf((*func(*int) bool)(nil)).Elem()) {
		t.Fatalf("exec.IsInputRegistered(reflect.TypeOf(((*func(*int) bool)(nil)).Elem()) = false, want true")
	}
}

func TestIter2(t *testing.T) {
	Iter2[int, string]()
	if !exec.IsInputRegistered(reflect.TypeOf((*func(*int, *string) bool)(nil)).Elem()) {
		t.Fatalf("exec.IsInputRegistered(reflect.TypeOf((*func(*int, *string) bool)(nil)).Elem()) = false, want true")
	}
}

func TestIter1_Struct(t *testing.T) {
	values := []exec.FullValue{exec.FullValue{
		Windows:   window.SingleGlobalWindow,
		Timestamp: mtime.ZeroTimestamp,
		Elm:       "one",
	}, exec.FullValue{
		Windows:   window.SingleGlobalWindow,
		Timestamp: mtime.ZeroTimestamp,
		Elm:       "two",
	}, exec.FullValue{
		Windows:   window.SingleGlobalWindow,
		Timestamp: mtime.ZeroTimestamp,
		Elm:       "three",
	}}

	i := iter1[string]{s: &exec.FixedReStream{Buf: values}}

	i.Init()
	fn := i.Value().(func(value *string) bool)

	var s string
	if ok := fn(&s); !ok {
		t.Fatalf("First i.Value()(&s)=false, want true")
	}
	if got, want := s, "one"; got != want {
		t.Fatalf("First iter value = %v, want %v", got, want)
	}
	if ok := fn(&s); !ok {
		t.Fatalf("Second i.Value()(&s)=false, want true")
	}
	if got, want := s, "two"; got != want {
		t.Fatalf("Second iter value = %v, want %v", got, want)
	}
	if ok := fn(&s); !ok {
		t.Fatalf("Third i.Value()(&s)=false, want true")
	}
	if got, want := s, "three"; got != want {
		t.Fatalf("Third iter value = %v, want %v", got, want)
	}
	if ok := fn(&s); ok {
		t.Fatalf("Fourth i.Value()(&s)=true, want false")
	}
	if err := i.Reset(); err != nil {
		t.Fatalf("i.Reset()=%v, want nil", err)
	}
}

func TestIter2_Struct(t *testing.T) {
	values := []exec.FullValue{exec.FullValue{
		Windows:   window.SingleGlobalWindow,
		Timestamp: mtime.ZeroTimestamp,
		Elm:       1,
		Elm2:      "one",
	}, exec.FullValue{
		Windows:   window.SingleGlobalWindow,
		Timestamp: mtime.ZeroTimestamp,
		Elm:       2,
		Elm2:      "two",
	}, exec.FullValue{
		Windows:   window.SingleGlobalWindow,
		Timestamp: mtime.ZeroTimestamp,
		Elm:       3,
		Elm2:      "three",
	}}

	i := iter2[int, string]{s: &exec.FixedReStream{Buf: values}}

	i.Init()
	fn := i.Value().(func(key *int, value *string) bool)

	var s string
	var key int
	if ok := fn(&key, &s); !ok {
		t.Fatalf("First i.Value()(&s)=false, want true")
	}
	if got, want := key, 1; got != want {
		t.Fatalf("First iter key = %v, want %v", got, want)
	}
	if got, want := s, "one"; got != want {
		t.Fatalf("First iter value = %v, want %v", got, want)
	}
	if ok := fn(&key, &s); !ok {
		t.Fatalf("Second i.Value()(&s)=false, want true")
	}
	if got, want := key, 2; got != want {
		t.Fatalf("Second iter key = %v, want %v", got, want)
	}
	if got, want := s, "two"; got != want {
		t.Fatalf("Second iter value = %v, want %v", got, want)
	}
	if ok := fn(&key, &s); !ok {
		t.Fatalf("Third i.Value()(&s)=false, want true")
	}
	if got, want := key, 3; got != want {
		t.Fatalf("Third iter key = %v, want %v", got, want)
	}
	if got, want := s, "three"; got != want {
		t.Fatalf("Third iter value = %v, want %v", got, want)
	}
	if ok := fn(&key, &s); ok {
		t.Fatalf("Fourth i.Value()(&s)=true, want false")
	}
	if err := i.Reset(); err != nil {
		t.Fatalf("i.Reset()=%v, want nil", err)
	}
}

type elementProcessor struct {
	inFV exec.FullValue
}

func (e *elementProcessor) ProcessElement(ctx context.Context, elm *exec.FullValue, values ...exec.ReStream) error {
	e.inFV = *elm
	return nil
}

type CompleteDoFn2x1 struct {
}

func (fn *CompleteDoFn2x1) ProcessElement(word string, emit func(int)) int {
	emit(len(word))
	return len(word)
}

func (fn *CompleteDoFn2x1) StartBundle(_ context.Context, emit func(int)) {
	emit(2)
}

func (fn *CompleteDoFn2x1) FinishBundle(_ context.Context, emit func(int)) error {
	emit(2)
	return nil
}

func (fn *CompleteDoFn2x1) Setup(_ context.Context) error {
	return nil
}

func (fn *CompleteDoFn2x1) Teardown() error {
	return nil
}

type PartialDoFn1x0 struct {
}

func (fn *PartialDoFn1x0) ProcessElement(emit func(int)) {
	emit(5)
}

func (fn *PartialDoFn1x0) StartBundle(_ context.Context, emit func(int)) {
	emit(2)
}

type ProcessElementDoFn0x1 struct {
}

func (fn *ProcessElementDoFn0x1) ProcessElement() int {
	return 5
}

type CustomType struct {
	val int
}

type CustomType2 struct {
	val2 int
}

type CustomTypeDoFn1x1 struct {
}

func (fn *CustomTypeDoFn1x1) ProcessElement(t CustomType) CustomType2 {
	return CustomType2{val2: t.val}
}

// Foo is a struct with a method for measuring method invocation
// overhead for StructuralDoFns.
type Foo struct {
	A int
}

// ProcessElement is a method for measuring a baseline of structural dofn overhead.
func (f *Foo) ProcessElement(b CustomType) int {
	return f.A + b.val
}

func MakeMultiEdge(f *graph.DoFn) graph.MultiEdge {
	return graph.MultiEdge{
		DoFn: f,
	}
}

type callerCustomTypeГint struct {
	fn func(CustomType) int
}

func funcMakerCustomTypeГint(fn interface{}) reflectx.Func {
	f := fn.(func(CustomType) int)
	return &callerCustomTypeГint{fn: f}
}

func (c *callerCustomTypeГint) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerCustomTypeГint) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerCustomTypeГint) Call(args []interface{}) []interface{} {
	out0 := c.fn(args[0].(CustomType))
	return []interface{}{out0}
}

func (c *callerCustomTypeГint) Call1x1(arg0 interface{}) interface{} {
	return c.fn(arg0.(CustomType))
}

func wrapMakerFoo(fn interface{}) map[string]reflectx.Func {
	dfn := fn.(*Foo)
	return map[string]reflectx.Func{
		"ProcessElement": reflectx.MakeFunc(func(a0 CustomType) int { return dfn.ProcessElement(a0) }),
	}
}

func GeneratedOptimizationCalls() {
	runtime.RegisterType(reflect.TypeOf((*Foo)(nil)).Elem())
	schema.RegisterType(reflect.TypeOf((*Foo)(nil)).Elem())
	runtime.RegisterType(reflect.TypeOf((*CustomType)(nil)).Elem())
	schema.RegisterType(reflect.TypeOf((*CustomType)(nil)).Elem())
	reflectx.RegisterFunc(reflect.TypeOf((*func(CustomType) int)(nil)).Elem(), funcMakerCustomTypeГint)
	reflectx.RegisterStructWrapper(reflect.TypeOf((*Foo)(nil)).Elem(), wrapMakerFoo)
}

// BenchmarkMethodCalls measures the overhead of invoking several different methods after performing
// different types of registration. The unoptimized calls don't perform any optimization. The
// GenericRegistration calls first register the DoFn being used with this package's generic registration
// functions. This is the preferred path for users. The GeneratedShims calls call various registration
// functions, mirroring the behavior of the shims generated by the code generator. This is not the
// recommended path for most users - if these are materially better than the generic benchmarks,
// this package requires further optimization.
//
// BenchmarkMethodCalls/MakeFunc_Unoptimized-16          			11480814	        88.35 ns/op
// BenchmarkMethodCalls/NewFn_Unoptimized-16            	  		 875199	      		1385 ns/op
// BenchmarkMethodCalls/EncodeMultiEdge_Unoptimized-16  	 		 1000000	      	1063 ns/op
//
// BenchmarkMethodCalls/MakeFunc_GenericRegistration-16  			16266259	        72.07 ns/op
// BenchmarkMethodCalls/NewFn_GenericRegistration-16    	 		 1000000	      	1108 ns/op
// BenchmarkMethodCalls/EncodeMultiEdge_GenericRegistration-16       1000000	      	1052 ns/op
//
// BenchmarkMethodCalls/MakeFunc_GeneratedShims#01-16               16400914	        69.17 ns/op
// BenchmarkMethodCalls/NewFn_GeneratedShims#01-16                   1000000	      	1099 ns/op
// BenchmarkMethodCalls/EncodeMultiEdge_GeneratedShims#01-16         1000000	      	1071 ns/op
func BenchmarkMethodCalls(b *testing.B) {
	f := &Foo{A: 3}
	g, err := graph.NewFn(&Foo{A: 5})
	if err != nil {
		panic(err)
	}
	gDoFn, err := graph.AsDoFn(g, 1)
	if err != nil {
		panic(err)
	}
	me := MakeMultiEdge(gDoFn)

	// Parameters
	var aFunc reflectx.Func
	var aFn *graph.Fn
	var aME interface{}

	// We need to do this registration just to get it to not panic when encoding the multi-edge with no additional optimization
	runtime.RegisterType(reflect.TypeOf(&f).Elem())
	tests := []struct {
		name         string
		fn           func()
		registration func()
	}{
		// No optimization performed at all
		{"MakeFunc_Unoptimized", func() { aFunc = reflectx.MakeFunc(f.ProcessElement) }, func() { /*No op*/ }}, // Used in graph deserialization
		{"NewFn_Unoptimized", func() { aFn, _ = graph.NewFn(f) }, func() { /*No op*/ }},                        // Used in graph construction (less valuable)
		{"EncodeMultiEdge_Unoptimized", func() { aME, _ = graphx.EncodeMultiEdge(&me) }, func() { /*No op*/ }}, // Used in graph serialization at execution time

		// Perform some generic registration to optimize execution
		{"MakeFunc_GenericRegistration", func() { aFunc = reflectx.MakeFunc(f.ProcessElement) }, func() { DoFn1x1[CustomType, int](f) }}, // Used in graph deserialization
		{"NewFn_GenericRegistration", func() { aFn, _ = graph.NewFn(f) }, func() { DoFn1x1[CustomType, int](f) }},                        // Used in graph construction (less valuable)
		{"EncodeMultiEdge_GenericRegistration", func() { aME, _ = graphx.EncodeMultiEdge(&me) }, func() { DoFn1x1[CustomType, int](f) }}, // Used in graph serialization at execution time

		// Perform some registration via copies of the code generator's shims
		{"MakeFunc_GeneratedShims", func() { aFunc = reflectx.MakeFunc(f.ProcessElement) }, func() { GeneratedOptimizationCalls() }},   // Used in graph deserialization
		{"NewFn_GeneratedShims", func() { aFn, _ = graph.NewFn(f) }, func() { GeneratedOptimizationCalls() }},                          // Used in graph construction (less valuable)
		{"EncodeMultiEdge_GeneratedShims", func() { aME, err = graphx.EncodeMultiEdge(&me) }, func() { GeneratedOptimizationCalls() }}, // Used in graph serialization at execution time
	}
	for _, test := range tests {
		test.registration()
		b.Run(test.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				test.fn()
			}
		})
	}
	b.Log(aFunc)
	b.Log(aFn)
	b.Log(aME)
}
