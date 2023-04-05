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

package register

import (
	"context"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
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
	if got, want := pe.Call([]any{"hello", emit})[0].(int), 5; got != want {
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
	sb.Call([]any{context.Background(), emit})
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
	if got := fb.Call([]any{context.Background(), emit})[0]; got != nil {
		t.Errorf("Wrapped FinishBundle call: got %v, want nil", got)
	}
	if got, want := val, 2; got != want {
		t.Errorf("Wrapped FinishBundle call emit result: got %v, want %v", got, want)
	}
	su, ok := m["Setup"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(CompleteDoFn2x1{}), no registered entry found for Setup")
	}
	if got := su.Call([]any{context.Background()})[0]; got != nil {
		t.Errorf("Wrapped FinishBundle call: got %v, want nil", got)
	}
	td, ok := m["Teardown"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(CompleteDoFn2x1{}), no registered entry found for Teardown")
	}
	if got := td.Call([]any{})[0]; got != nil {
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
	pe.Call([]any{emit})
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
	sb.Call([]any{context.Background(), emit})
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
	if got, want := pe.Call([]any{})[0].(int), 5; got != want {
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

func TestCombiner_CompleteCombiner3(t *testing.T) {
	accum := &CompleteCombiner3{}
	Combiner3[int, CustomType, CustomType2](accum)

	m, ok := reflectx.WrapMethods(&CompleteCombiner3{})
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteCombiner3{}), no registered entry found")
	}
	ca, ok := m["CreateAccumulator"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteCombiner3{}), no registered entry found for CreateAccumulator")
	}
	if got, want := ca.Call([]any{})[0].(int), 0; got != want {
		t.Errorf("Wrapped CreateAccumulator call: got %v, want %v", got, want)
	}
	ai, ok := m["AddInput"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteCombiner3{}), no registered entry found for AddInput")
	}
	if got, want := ai.Call([]any{2, CustomType{val: 3}})[0].(int), 5; got != want {
		t.Errorf("Wrapped AddInput call: got %v, want %v", got, want)
	}
	ma, ok := m["MergeAccumulators"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteCombiner3{}), no registered entry found for MergeAccumulators")
	}
	if got, want := ma.Call([]any{2, 4})[0].(int), 6; got != want {
		t.Errorf("Wrapped MergeAccumulators call: got %v, want %v", got, want)
	}
	eo, ok := m["ExtractOutput"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteCombiner3{}), no registered entry found for MergeAccumulators")
	}
	if got, want := eo.Call([]any{2})[0].(CustomType2).val2, 2; got != want {
		t.Errorf("Wrapped MergeAccumulators call: got %v, want %v", got, want)
	}
}

func TestCombiner_RegistersTypes(t *testing.T) {
	accum := &CompleteCombiner3{}
	Combiner3[int, CustomType, CustomType2](accum)

	// Need to call FromType so that the registry will reconcile its registrations
	schema.FromType(reflect.TypeOf(accum).Elem())
	if !schema.Registered(reflect.TypeOf(accum).Elem()) {
		t.Errorf("schema.Registered(reflect.TypeOf(CustomTypeDoFn1x1{})) = false, want true")
	}
	if !schema.Registered(reflect.TypeOf(CustomType{})) {
		t.Errorf("schema.Registered(reflect.TypeOf(CustomType{})) = false, want true")
	}
	if !schema.Registered(reflect.TypeOf(CustomType2{})) {
		t.Errorf("schema.Registered(reflect.TypeOf(CustomType{})) = false, want true")
	}
}

func TestCombiner_CompleteCombiner2(t *testing.T) {
	accum := &CompleteCombiner2{}
	Combiner2[int, CustomType](accum)

	m, ok := reflectx.WrapMethods(&CompleteCombiner2{})
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteCombiner2{}), no registered entry found")
	}
	ca, ok := m["CreateAccumulator"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteCombiner2{}), no registered entry found for CreateAccumulator")
	}
	if got, want := ca.Call([]any{})[0].(int), 0; got != want {
		t.Errorf("Wrapped CreateAccumulator call: got %v, want %v", got, want)
	}
	ai, ok := m["AddInput"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteCombiner2{}), no registered entry found for AddInput")
	}
	if got, want := ai.Call([]any{2, CustomType{val: 3}})[0].(int), 5; got != want {
		t.Errorf("Wrapped AddInput call: got %v, want %v", got, want)
	}
	ma, ok := m["MergeAccumulators"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteCombiner2{}), no registered entry found for MergeAccumulators")
	}
	if got, want := ma.Call([]any{2, 4})[0].(int), 6; got != want {
		t.Errorf("Wrapped MergeAccumulators call: got %v, want %v", got, want)
	}
	eo, ok := m["ExtractOutput"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteCombiner2{}), no registered entry found for MergeAccumulators")
	}
	if got, want := eo.Call([]any{2})[0].(CustomType).val, 2; got != want {
		t.Errorf("Wrapped MergeAccumulators call: got %v, want %v", got, want)
	}
}

func TestCombiner_CompleteCombiner1(t *testing.T) {
	accum := &CompleteCombiner1{}
	Combiner1[int](accum)

	m, ok := reflectx.WrapMethods(&CompleteCombiner1{})
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteCombiner1{}), no registered entry found")
	}
	ca, ok := m["CreateAccumulator"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteCombiner1{}), no registered entry found for CreateAccumulator")
	}
	if got, want := ca.Call([]any{})[0].(int), 0; got != want {
		t.Errorf("Wrapped CreateAccumulator call: got %v, want %v", got, want)
	}
	ai, ok := m["AddInput"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteCombiner1{}), no registered entry found for AddInput")
	}
	if got, want := ai.Call([]any{2, 3})[0].(int), 5; got != want {
		t.Errorf("Wrapped AddInput call: got %v, want %v", got, want)
	}
	ma, ok := m["MergeAccumulators"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteCombiner1{}), no registered entry found for MergeAccumulators")
	}
	if got, want := ma.Call([]any{2, 4})[0].(int), 6; got != want {
		t.Errorf("Wrapped MergeAccumulators call: got %v, want %v", got, want)
	}
	eo, ok := m["ExtractOutput"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteCombiner1{}), no registered entry found for MergeAccumulators")
	}
	if got, want := eo.Call([]any{2})[0].(int), 2; got != want {
		t.Errorf("Wrapped MergeAccumulators call: got %v, want %v", got, want)
	}
}

func TestCombiner_PartialCombiner2(t *testing.T) {
	accum := &PartialCombiner2{}
	Combiner2[int, CustomType](accum)

	m, ok := reflectx.WrapMethods(&PartialCombiner2{})
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&PartialCombiner2{}), no registered entry found")
	}
	ca, ok := m["CreateAccumulator"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&PartialCombiner2{}), no registered entry found for CreateAccumulator")
	}
	if got, want := ca.Call([]any{})[0].(int), 0; got != want {
		t.Errorf("Wrapped CreateAccumulator call: got %v, want %v", got, want)
	}
	ai, ok := m["AddInput"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&PartialCombiner2{}), no registered entry found for AddInput")
	}
	if got, want := ai.Call([]any{2, CustomType{val: 3}})[0].(int), 5; got != want {
		t.Errorf("Wrapped AddInput call: got %v, want %v", got, want)
	}
	ma, ok := m["MergeAccumulators"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&PartialCombiner2{}), no registered entry found for MergeAccumulators")
	}
	if got, want := ma.Call([]any{2, 4})[0].(int), 6; got != want {
		t.Errorf("Wrapped MergeAccumulators call: got %v, want %v", got, want)
	}
}

type CustomFunctionParameter struct {
	key string
	val int
}

type CustomFunctionReturn struct {
	key int
	val string
}

func customFunction(a CustomFunctionParameter) CustomFunctionReturn {
	return CustomFunctionReturn{
		key: a.val,
		val: a.key,
	}
}

func TestFunction(t *testing.T) {
	Function1x1[CustomFunctionParameter, CustomFunctionReturn](customFunction)

	// Need to call FromType so that the registry will reconcile its registrations
	schema.FromType(reflect.TypeOf(CustomFunctionParameter{}))
	if !schema.Registered(reflect.TypeOf(CustomFunctionParameter{})) {
		t.Errorf("schema.Registered(reflect.TypeOf(CustomFunctionParameter{})) = false, want true")
	}
	if !schema.Registered(reflect.TypeOf(CustomFunctionReturn{})) {
		t.Errorf("schema.Registered(reflect.TypeOf(CustomFunctionReturn{})) = false, want true")
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

type CompleteCombiner3 struct {
}

func (fn *CompleteCombiner3) CreateAccumulator() int {
	return 0
}

func (fn *CompleteCombiner3) AddInput(i int, c CustomType) int {
	return i + c.val
}

func (fn *CompleteCombiner3) MergeAccumulators(i1 int, i2 int) int {
	return i1 + i2
}

func (fn *CompleteCombiner3) ExtractOutput(i int) CustomType2 {
	return CustomType2{val2: i}
}

type CompleteCombiner2 struct {
}

func (fn *CompleteCombiner2) CreateAccumulator() int {
	return 0
}

func (fn *CompleteCombiner2) AddInput(i int, c CustomType) int {
	return i + c.val
}

func (fn *CompleteCombiner2) MergeAccumulators(i1 int, i2 int) int {
	return i1 + i2
}

func (fn *CompleteCombiner2) ExtractOutput(i int) CustomType {
	return CustomType{val: i}
}

type CompleteCombiner1 struct {
}

func (fn *CompleteCombiner1) CreateAccumulator() int {
	return 0
}

func (fn *CompleteCombiner1) AddInput(i1 int, i2 int) int {
	return i1 + i2
}

func (fn *CompleteCombiner1) MergeAccumulators(i1 int, i2 int) int {
	return i1 + i2
}

func (fn *CompleteCombiner1) ExtractOutput(i int) int {
	return i
}

type PartialCombiner2 struct {
}

func (fn *PartialCombiner2) CreateAccumulator() int {
	return 0
}

func (fn *PartialCombiner2) AddInput(i int, c CustomType) int {
	return i + c.val
}

func (fn *PartialCombiner2) MergeAccumulators(i1 int, i2 int) int {
	return i1 + i2
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

func (c *callerCustomTypeГint) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerCustomTypeГint) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerCustomTypeГint) Call(args []any) []any {
	out0 := c.fn(args[0].(CustomType))
	return []any{out0}
}

func (c *callerCustomTypeГint) Call1x1(arg0 any) any {
	return c.fn(arg0.(CustomType))
}

type callerCustomType2CustomType2ГCustomType2 struct {
	fn func(CustomType2, CustomType2) CustomType2
}

func (c *callerCustomType2CustomType2ГCustomType2) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerCustomType2CustomType2ГCustomType2) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerCustomType2CustomType2ГCustomType2) Call(args []any) []any {
	out0 := c.fn(args[0].(CustomType2), args[0].(CustomType2))
	return []any{out0}
}

func (c *callerCustomType2CustomType2ГCustomType2) Call2x1(arg0, arg1 any) any {
	return c.fn(arg0.(CustomType2), arg1.(CustomType2))
}

func funcMakerCustomTypeГint(fn any) reflectx.Func {
	f := fn.(func(CustomType) int)
	return &callerCustomTypeГint{fn: f}
}

func funcMakerCustomType2CustomType2ГCustomType2(fn any) reflectx.Func {
	f := fn.(func(CustomType2, CustomType2) CustomType2)
	return &callerCustomType2CustomType2ГCustomType2{fn: f}
}

func wrapMakerFoo(fn any) map[string]reflectx.Func {
	dfn := fn.(*Foo)
	return map[string]reflectx.Func{
		"ProcessElement": reflectx.MakeFunc(func(a0 CustomType) int { return dfn.ProcessElement(a0) }),
	}
}

func addCustomType2(a CustomType2, b CustomType2) CustomType2 {
	return CustomType2{
		val2: a.val2 + b.val2,
	}
}

func GeneratedOptimizationCalls() {
	runtime.RegisterType(reflect.TypeOf((*Foo)(nil)).Elem())
	schema.RegisterType(reflect.TypeOf((*Foo)(nil)).Elem())
	runtime.RegisterType(reflect.TypeOf((*CustomType)(nil)).Elem())
	schema.RegisterType(reflect.TypeOf((*CustomType)(nil)).Elem())
	runtime.RegisterType(reflect.TypeOf((*CustomType2)(nil)).Elem())
	schema.RegisterType(reflect.TypeOf((*CustomType2)(nil)).Elem())
	reflectx.RegisterFunc(reflect.TypeOf((*func(CustomType) int)(nil)).Elem(), funcMakerCustomTypeГint)
	reflectx.RegisterFunc(reflect.TypeOf((*func(CustomType2, CustomType2) CustomType2)(nil)).Elem(), funcMakerCustomType2CustomType2ГCustomType2)
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
// BenchmarkMethodCalls/MakeFunc_Unoptimized-16                                     11480814	        88.35 ns/op
// BenchmarkMethodCalls/MakeFunc.Call_Unoptimized-16                                3525211	            324.0 ns/op
// BenchmarkMethodCalls/MakeFunc.Call1x1_Unoptimized-16                             3450822	            343.0 ns/op
// BenchmarkMethodCalls/NewFn_Unoptimized-16                                        875199	      		1385 ns/op
// BenchmarkMethodCalls/EncodeMultiEdge_Unoptimized-16                              1000000	      	    1063 ns/op
// BenchmarkMethodCalls/MakeFunc_FunctionalDoFn_Unoptimized-16         	            11984484	        110.6 ns/op
// BenchmarkMethodCalls/MakeFunc_FunctionalDoFn.Call_Unoptimized-16    	            1574622	            744.4 ns/op
// BenchmarkMethodCalls/MakeFunc_FunctionalDoFn.Call1x1_Unoptimized-16 	            1504969	            795.9 ns/op
//
// BenchmarkMethodCalls/MakeFunc_GenericRegistration-16                             16266259	        72.07 ns/op
// BenchmarkMethodCalls/MakeFunc.Call_GenericRegistration-16                        38331327	        32.70 ns/op
// BenchmarkMethodCalls/MakeFunc.Call1x1_GenericRegistration-16                     135934086	        8.434 ns/op
// BenchmarkMethodCalls/NewFn_GenericRegistration-16                                1000000	            1108 ns/op
// BenchmarkMethodCalls/EncodeMultiEdge_GenericRegistration-16                      1000000	            1052 ns/op
// BenchmarkMethodCalls/MakeFunc_FunctionalDoFn_GenericRegistration-16 	            11295202	        95.43 ns/op
// BenchmarkMethodCalls/MakeFunc_FunctionalDoFn.Call_GenericRegistration-16         20299956	        54.15 ns/op
// BenchmarkMethodCalls/MakeFunc_FunctionalDoFn.Call1x1_GenericRegistration-16      92858212	        12.86 ns/op
//
// BenchmarkMethodCalls/MakeFunc_GeneratedShims-16                                  16400914	        69.17 ns/op
// BenchmarkMethodCalls/MakeFunc.Call_GeneratedShims-16                             37106445	        33.69 ns/op
// BenchmarkMethodCalls/MakeFunc.Call1x1_GeneratedShims-16                          141127965	        8.312 ns/op
// BenchmarkMethodCalls/NewFn_GeneratedShims-16                                     1000000	        	1099 ns/op
// BenchmarkMethodCalls/EncodeMultiEdge_GeneratedShims-16                           1000000 	      	1071 ns/op
// BenchmarkMethodCalls/MakeFunc_FunctionalDoFn_GeneratedShims-16                   12444930	        90.77 ns/op
// BenchmarkMethodCalls/MakeFunc_FunctionalDoFn.Call_GeneratedShims-16              19462878	        51.92 ns/op
// BenchmarkMethodCalls/MakeFunc_FunctionalDoFn.Call2x1_GeneratedShims-16           85194289	        15.76 ns/op
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

	var aFunc reflectx.Func
	var aFn *graph.Fn
	var aME any
	var aFnCall int
	var aFnCall2 CustomType2
	var aFunc1x1 reflectx.Func1x1
	var aFunc2x1 reflectx.Func2x1
	funcIn := []any{CustomType{val: 4}}
	funcIn2 := []any{CustomType2{val2: 4}, CustomType2{val2: 3}}

	// We need to do this registration just to get it to not panic when encoding the multi-edge with no additional optimization.
	// This is currently required of users anyways
	runtime.RegisterType(reflect.TypeOf((*Foo)(nil)))
	tests := []struct {
		name         string
		fn           func()
		registration func()
	}{
		// No optimization performed at all
		{"MakeFunc_Unoptimized", func() { aFunc = reflectx.MakeFunc(f.ProcessElement) }, func() { /*No op*/ }},                                                                                             // Used in graph deserialization
		{"MakeFunc.Call_Unoptimized", func() { aFnCall = aFunc.Call(funcIn)[0].(int) }, func() { /*No op*/ }},                                                                                              // Used to call the function repeatedly
		{"MakeFunc.Call1x1_Unoptimized", func() { aFnCall = aFunc1x1.Call1x1(CustomType{val: 4}).(int) }, func() { aFunc1x1 = reflectx.ToFunc1x1(aFunc) }},                                                 // Used to call the function repeatedly
		{"NewFn_Unoptimized", func() { aFn, _ = graph.NewFn(f) }, func() { /*No op*/ }},                                                                                                                    // Used in graph construction (less valuable)
		{"EncodeMultiEdge_Unoptimized", func() { aME, _ = graphx.EncodeMultiEdge(&me) }, func() { /*No op*/ }},                                                                                             // Used in graph serialization at execution time
		{"MakeFunc_FunctionalDoFn_Unoptimized", func() { aFunc = reflectx.MakeFunc(addCustomType2) }, func() { /*No op*/ }},                                                                                // Used in graph deserialization
		{"MakeFunc_FunctionalDoFn.Call_Unoptimized", func() { aFnCall2 = aFunc.Call(funcIn2)[0].(CustomType2) }, func() { /*No op*/ }},                                                                     // Used to call the function repeatedly
		{"MakeFunc_FunctionalDoFn.Call2x1_Unoptimized", func() { aFnCall2 = aFunc2x1.Call2x1(CustomType2{val2: 4}, CustomType2{val2: 3}).(CustomType2) }, func() { aFunc2x1 = reflectx.ToFunc2x1(aFunc) }}, // Used to call the function repeatedly

		// Perform some generic registration to optimize execution
		{"MakeFunc_GenericRegistration", func() { aFunc = reflectx.MakeFunc(f.ProcessElement) }, func() { DoFn1x1[CustomType, int](f) }},                                                                // Used in graph deserialization
		{"MakeFunc.Call_GenericRegistration", func() { aFnCall = aFunc.Call(funcIn)[0].(int) }, func() { DoFn1x1[CustomType, int](f) }},                                                                 // Used to call the function repeatedly
		{"MakeFunc.Call1x1_GenericRegistration", func() { aFnCall = aFunc1x1.Call1x1(CustomType{val: 3}).(int) }, func() { DoFn1x1[CustomType, int](f); aFunc1x1 = reflectx.ToFunc1x1(aFunc) }},         // Used to call the function repeatedly
		{"NewFn_GenericRegistration", func() { aFn, _ = graph.NewFn(f) }, func() { DoFn1x1[CustomType, int](f) }},                                                                                       // Used in graph construction (less valuable)
		{"EncodeMultiEdge_GenericRegistration", func() { aME, _ = graphx.EncodeMultiEdge(&me) }, func() { DoFn1x1[CustomType, int](f) }},                                                                // Used in graph serialization at execution time
		{"MakeFunc_FunctionalDoFn_GenericRegistration", func() { aFunc = reflectx.MakeFunc(addCustomType2) }, func() { Function2x1[CustomType2, CustomType2, CustomType2](addCustomType2) }},            // Used in graph deserialization
		{"MakeFunc_FunctionalDoFn.Call_GenericRegistration", func() { aFnCall2 = aFunc.Call(funcIn2)[0].(CustomType2) }, func() { Function2x1[CustomType2, CustomType2, CustomType2](addCustomType2) }}, // Used to call the function repeatedly
		{"MakeFunc_FunctionalDoFn.Call2x1_GenericRegistration", func() { aFnCall2 = aFunc2x1.Call2x1(CustomType2{val2: 4}, CustomType2{val2: 3}).(CustomType2) }, func() {
			Function2x1[CustomType2, CustomType2, CustomType2](addCustomType2)
			aFunc2x1 = reflectx.ToFunc2x1(aFunc)
		}}, // Used to call the function repeatedly

		// Perform some registration via copies of the code generator's shims
		{"MakeFunc_GeneratedShims", func() { aFunc = reflectx.MakeFunc(f.ProcessElement) }, func() { GeneratedOptimizationCalls() }},                                                                                                        // Used in graph deserialization
		{"MakeFunc.Call_GeneratedShims", func() { aFnCall = aFunc.Call(funcIn)[0].(int) }, func() { GeneratedOptimizationCalls() }},                                                                                                         // Used to call the function repeatedly
		{"MakeFunc.Call1x1_GeneratedShims", func() { aFnCall = aFunc1x1.Call1x1(CustomType{val: 5}).(int) }, func() { GeneratedOptimizationCalls(); aFunc1x1 = reflectx.ToFunc1x1(aFunc) }},                                                 // Used to call the function repeatedly
		{"NewFn_GeneratedShims", func() { aFn, _ = graph.NewFn(f) }, func() { GeneratedOptimizationCalls() }},                                                                                                                               // Used in graph construction (less valuable)
		{"EncodeMultiEdge_GeneratedShims", func() { aME, err = graphx.EncodeMultiEdge(&me) }, func() { GeneratedOptimizationCalls() }},                                                                                                      // Used in graph serialization at execution time
		{"MakeFunc_FunctionalDoFn_GeneratedShims", func() { aFunc = reflectx.MakeFunc(addCustomType2) }, func() { GeneratedOptimizationCalls() }},                                                                                           // Used in graph deserialization
		{"MakeFunc_FunctionalDoFn.Call_GeneratedShims", func() { aFnCall2 = aFunc.Call(funcIn2)[0].(CustomType2) }, func() { GeneratedOptimizationCalls() }},                                                                                // Used to call the function repeatedly
		{"MakeFunc_FunctionalDoFn.Call2x1_GeneratedShims", func() { aFnCall2 = aFunc2x1.Call2x1(CustomType2{val2: 4}, CustomType2{val2: 3}).(CustomType2) }, func() { GeneratedOptimizationCalls(); aFunc2x1 = reflectx.ToFunc2x1(aFunc) }}, // Used to call the function repeatedly
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
	b.Log(aFnCall)
	b.Log(aFnCall2)
	b.Log(aFn)
	b.Log(aME)
}
