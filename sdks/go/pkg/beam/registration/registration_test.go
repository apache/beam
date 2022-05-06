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

func TestAccumulator_CompleteAccumulator3(t *testing.T) {
	accum := &CompleteAccumulator3{}
	Accumulator3[int, CustomType, CustomType2](accum)

	m, ok := reflectx.WrapMethods(&CompleteAccumulator3{})
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteAccumulator3{}), no registered entry found")
	}
	ca, ok := m["CreateAccumulator"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteAccumulator3{}), no registered entry found for CreateAccumulator")
	}
	if got, want := ca.Call([]interface{}{})[0].(int), 0; got != want {
		t.Errorf("Wrapped CreateAccumulator call: got %v, want %v", got, want)
	}
	ai, ok := m["AddInput"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteAccumulator3{}), no registered entry found for AddInput")
	}
	if got, want := ai.Call([]interface{}{2, CustomType{val: 3}})[0].(int), 5; got != want {
		t.Errorf("Wrapped AddInput call: got %v, want %v", got, want)
	}
	ma, ok := m["MergeAccumulators"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteAccumulator3{}), no registered entry found for MergeAccumulators")
	}
	if got, want := ma.Call([]interface{}{2, 4})[0].(int), 6; got != want {
		t.Errorf("Wrapped MergeAccumulators call: got %v, want %v", got, want)
	}
	eo, ok := m["ExtractOutput"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteAccumulator3{}), no registered entry found for MergeAccumulators")
	}
	if got, want := eo.Call([]interface{}{2})[0].(CustomType2).val2, 2; got != want {
		t.Errorf("Wrapped MergeAccumulators call: got %v, want %v", got, want)
	}
}

func TestAccumulator_RegistersTypes(t *testing.T) {
	accum := &CompleteAccumulator3{}
	Accumulator3[int, CustomType, CustomType2](accum)

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

func TestAccumulator_CompleteAccumulator2(t *testing.T) {
	accum := &CompleteAccumulator2{}
	Accumulator2[int, CustomType](accum)

	m, ok := reflectx.WrapMethods(&CompleteAccumulator2{})
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteAccumulator2{}), no registered entry found")
	}
	ca, ok := m["CreateAccumulator"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteAccumulator2{}), no registered entry found for CreateAccumulator")
	}
	if got, want := ca.Call([]interface{}{})[0].(int), 0; got != want {
		t.Errorf("Wrapped CreateAccumulator call: got %v, want %v", got, want)
	}
	ai, ok := m["AddInput"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteAccumulator2{}), no registered entry found for AddInput")
	}
	if got, want := ai.Call([]interface{}{2, CustomType{val: 3}})[0].(int), 5; got != want {
		t.Errorf("Wrapped AddInput call: got %v, want %v", got, want)
	}
	ma, ok := m["MergeAccumulators"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteAccumulator2{}), no registered entry found for MergeAccumulators")
	}
	if got, want := ma.Call([]interface{}{2, 4})[0].(int), 6; got != want {
		t.Errorf("Wrapped MergeAccumulators call: got %v, want %v", got, want)
	}
	eo, ok := m["ExtractOutput"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteAccumulator2{}), no registered entry found for MergeAccumulators")
	}
	if got, want := eo.Call([]interface{}{2})[0].(CustomType).val, 2; got != want {
		t.Errorf("Wrapped MergeAccumulators call: got %v, want %v", got, want)
	}
}

func TestAccumulator_CompleteAccumulator1(t *testing.T) {
	accum := &CompleteAccumulator1{}
	Accumulator1[int](accum)

	m, ok := reflectx.WrapMethods(&CompleteAccumulator1{})
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteAccumulator1{}), no registered entry found")
	}
	ca, ok := m["CreateAccumulator"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteAccumulator1{}), no registered entry found for CreateAccumulator")
	}
	if got, want := ca.Call([]interface{}{})[0].(int), 0; got != want {
		t.Errorf("Wrapped CreateAccumulator call: got %v, want %v", got, want)
	}
	ai, ok := m["AddInput"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteAccumulator1{}), no registered entry found for AddInput")
	}
	if got, want := ai.Call([]interface{}{2, 3})[0].(int), 5; got != want {
		t.Errorf("Wrapped AddInput call: got %v, want %v", got, want)
	}
	ma, ok := m["MergeAccumulators"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteAccumulator1{}), no registered entry found for MergeAccumulators")
	}
	if got, want := ma.Call([]interface{}{2, 4})[0].(int), 6; got != want {
		t.Errorf("Wrapped MergeAccumulators call: got %v, want %v", got, want)
	}
	eo, ok := m["ExtractOutput"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&CompleteAccumulator1{}), no registered entry found for MergeAccumulators")
	}
	if got, want := eo.Call([]interface{}{2})[0].(int), 2; got != want {
		t.Errorf("Wrapped MergeAccumulators call: got %v, want %v", got, want)
	}
}

func TestAccumulator_PartialAccumulator2(t *testing.T) {
	accum := &PartialAccumulator2{}
	Accumulator2[int, CustomType](accum)

	m, ok := reflectx.WrapMethods(&PartialAccumulator2{})
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&PartialAccumulator2{}), no registered entry found")
	}
	ca, ok := m["CreateAccumulator"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&PartialAccumulator2{}), no registered entry found for CreateAccumulator")
	}
	if got, want := ca.Call([]interface{}{})[0].(int), 0; got != want {
		t.Errorf("Wrapped CreateAccumulator call: got %v, want %v", got, want)
	}
	ai, ok := m["AddInput"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&PartialAccumulator2{}), no registered entry found for AddInput")
	}
	if got, want := ai.Call([]interface{}{2, CustomType{val: 3}})[0].(int), 5; got != want {
		t.Errorf("Wrapped AddInput call: got %v, want %v", got, want)
	}
	ma, ok := m["MergeAccumulators"]
	if !ok {
		t.Fatalf("reflectx.WrapMethods(&PartialAccumulator2{}), no registered entry found for MergeAccumulators")
	}
	if got, want := ma.Call([]interface{}{2, 4})[0].(int), 6; got != want {
		t.Errorf("Wrapped MergeAccumulators call: got %v, want %v", got, want)
	}
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

type CompleteAccumulator3 struct {
}

func (fn *CompleteAccumulator3) CreateAccumulator() int {
	return 0
}

func (fn *CompleteAccumulator3) AddInput(i int, c CustomType) int {
	return i + c.val
}

func (fn *CompleteAccumulator3) MergeAccumulators(i1 int, i2 int) int {
	return i1 + i2
}

func (fn *CompleteAccumulator3) ExtractOutput(i int) CustomType2 {
	return CustomType2{val2: i}
}

type CompleteAccumulator2 struct {
}

func (fn *CompleteAccumulator2) CreateAccumulator() int {
	return 0
}

func (fn *CompleteAccumulator2) AddInput(i int, c CustomType) int {
	return i + c.val
}

func (fn *CompleteAccumulator2) MergeAccumulators(i1 int, i2 int) int {
	return i1 + i2
}

func (fn *CompleteAccumulator2) ExtractOutput(i int) CustomType {
	return CustomType{val: i}
}

type CompleteAccumulator1 struct {
}

func (fn *CompleteAccumulator1) CreateAccumulator() int {
	return 0
}

func (fn *CompleteAccumulator1) AddInput(i1 int, i2 int) int {
	return i1 + i2
}

func (fn *CompleteAccumulator1) MergeAccumulators(i1 int, i2 int) int {
	return i1 + i2
}

func (fn *CompleteAccumulator1) ExtractOutput(i int) int {
	return i
}

type PartialAccumulator2 struct {
}

func (fn *PartialAccumulator2) CreateAccumulator() int {
	return 0
}

func (fn *PartialAccumulator2) AddInput(i int, c CustomType) int {
	return i + c.val
}

func (fn *PartialAccumulator2) MergeAccumulators(i1 int, i2 int) int {
	return i1 + i2
}
