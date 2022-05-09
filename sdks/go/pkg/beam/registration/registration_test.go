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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
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
