// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package registration

import (
	"context"
	"fmt"
	"io"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

type emitNative1[T any] struct {
	n  exec.ElementProcessor
	fn interface{}

	ctx   context.Context
	ws    []typex.Window
	et    typex.EventTime
	value exec.FullValue
}

func (e *emitNative1[T]) Init(ctx context.Context, ws []typex.Window, et typex.EventTime) error {
	e.ctx = ctx
	e.ws = ws
	e.et = et
	return nil
}

func (e *emitNative1[T]) Value() interface{} {
	return e.fn
}

func (e *emitNative1[T]) invoke(val T) {
	e.value = exec.FullValue{Windows: e.ws, Timestamp: e.et, Elm: val}
	if err := e.n.ProcessElement(e.ctx, &e.value); err != nil {
		panic(err)
	}
}

type emitNative2[T1, T2 any] struct {
	n  exec.ElementProcessor
	fn interface{}

	ctx   context.Context
	ws    []typex.Window
	et    typex.EventTime
	value exec.FullValue
}

func (e *emitNative2[T1, T2]) Init(ctx context.Context, ws []typex.Window, et typex.EventTime) error {
	e.ctx = ctx
	e.ws = ws
	e.et = et
	return nil
}

func (e *emitNative2[T1, T2]) Value() interface{} {
	return e.fn
}

func (e *emitNative2[T1, T2]) invoke(key T1, val T2) {
	e.value = exec.FullValue{Windows: e.ws, Timestamp: e.et, Elm: key, Elm2: val}
	if err := e.n.ProcessElement(e.ctx, &e.value); err != nil {
		panic(err)
	}
}

type emitNative1WithTimestamp[T any] struct {
	n  exec.ElementProcessor
	fn interface{}

	ctx   context.Context
	ws    []typex.Window
	et    typex.EventTime
	value exec.FullValue
}

func (e *emitNative1WithTimestamp[T]) Init(ctx context.Context, ws []typex.Window, et typex.EventTime) error {
	e.ctx = ctx
	e.ws = ws
	e.et = et
	return nil
}

func (e *emitNative1WithTimestamp[T]) Value() interface{} {
	return e.fn
}

func (e *emitNative1WithTimestamp[T]) invoke(et typex.EventTime, val T) {
	e.value = exec.FullValue{Windows: e.ws, Timestamp: et, Elm: val}
	if err := e.n.ProcessElement(e.ctx, &e.value); err != nil {
		panic(err)
	}
}

type emitNative2WithTimestamp[T1, T2 any] struct {
	n  exec.ElementProcessor
	fn interface{}

	ctx   context.Context
	ws    []typex.Window
	et    typex.EventTime
	value exec.FullValue
}

func (e *emitNative2WithTimestamp[T1, T2]) Init(ctx context.Context, ws []typex.Window, et typex.EventTime) error {
	e.ctx = ctx
	e.ws = ws
	e.et = et
	return nil
}

func (e *emitNative2WithTimestamp[T1, T2]) Value() interface{} {
	return e.fn
}

func (e *emitNative2WithTimestamp[T1, T2]) invoke(et typex.EventTime, key T1, val T2) {
	e.value = exec.FullValue{Windows: e.ws, Timestamp: et, Elm: key, Elm2: val}
	if err := e.n.ProcessElement(e.ctx, &e.value); err != nil {
		panic(err)
	}
}

// RegisterEmitter1 registers parameters from your DoFn with a
// signature func(T) and optimizes their execution.
// This must be done by passing in a reference to an instantiated version
// of the function, aka:
// registration.RegisterEmitter1[T]((*func(T))(nil))
func RegisterEmitter1[T1 any](e *func(T1)) {
	registerFunc := func(n exec.ElementProcessor) exec.ReusableEmitter {
		gen := &emitNative1[T1]{n: n}
		gen.fn = gen.invoke
		return gen
	}
	exec.RegisterEmitter(reflect.TypeOf(e).Elem(), registerFunc)
}

// RegisterEmitter2 registers parameters from your DoFn with a
// signature func(T1, T2) and optimizes their execution.
// This must be done by passing in a reference to an instantiated version
// of the function, aka:
// registration.RegisterEmitter2[T1, T2]((*func(T1, T2))(nil))
func RegisterEmitter2[T1, T2 any](e *func(T1, T2)) {
	registerFunc := func(n exec.ElementProcessor) exec.ReusableEmitter {
		gen := &emitNative2[T1, T2]{n: n}
		gen.fn = gen.invoke
		return gen
	}
	if reflect.TypeOf(e).Elem().In(0) == typex.EventTimeType {
		registerFunc = func(n exec.ElementProcessor) exec.ReusableEmitter {
			gen := &emitNative1WithTimestamp[T2]{n: n}
			gen.fn = gen.invoke
			return gen
		}
	}
	exec.RegisterEmitter(reflect.TypeOf(e).Elem(), registerFunc)
}

// RegisterEmitter3 registers parameters from your DoFn with a
// signature func(T1, T2, T3) and optimizes their execution.
// This must be done by passing in a reference to an instantiated version
// of the function, aka:
// registration.RegisterEmitter3[T1, T2, T3]((*func(T1, T2, T3))(nil))
func RegisterEmitter3[T1, T2, T3 any](e *func(T1, T2, T3)) {
	registerFunc := func(n exec.ElementProcessor) exec.ReusableEmitter {
		gen := &emitNative2WithTimestamp[T2, T3]{n: n}
		gen.fn = gen.invoke
		return gen
	}
	exec.RegisterEmitter(reflect.TypeOf(e).Elem(), registerFunc)
}

type iterNative1[T any] struct {
	s  exec.ReStream
	fn interface{}

	// cur is the "current" stream, if any.
	cur exec.Stream
}

func (v *iterNative1[T]) Init() error {
	cur, err := v.s.Open()
	if err != nil {
		return err
	}
	v.cur = cur
	return nil
}

func (v *iterNative1[T]) Value() interface{} {
	return v.fn
}

func (v *iterNative1[T]) Reset() error {
	if err := v.cur.Close(); err != nil {
		return err
	}
	v.cur = nil
	return nil
}

func (v *iterNative1[T]) invoke(value *T) bool {
	elm, err := v.cur.Read()
	if err != nil {
		if err == io.EOF {
			return false
		}
		panic(fmt.Sprintf("broken stream: %v", err))
	}
	*value = elm.Elm.(T)
	return true
}

type iterNative2[T1, T2 any] struct {
	s  exec.ReStream
	fn interface{}

	// cur is the "current" stream, if any.
	cur exec.Stream
}

func (v *iterNative2[T1, T2]) Init() error {
	cur, err := v.s.Open()
	if err != nil {
		return err
	}
	v.cur = cur
	return nil
}

func (v *iterNative2[T1, T2]) Value() interface{} {
	return v.fn
}

func (v *iterNative2[T1, T2]) Reset() error {
	if err := v.cur.Close(); err != nil {
		return err
	}
	v.cur = nil
	return nil
}

func (v *iterNative2[T1, T2]) invoke(key *T1, value *T2) bool {
	elm, err := v.cur.Read()
	if err != nil {
		if err == io.EOF {
			return false
		}
		panic(fmt.Sprintf("broken stream: %v", err))
	}
	*key = elm.Elm.(T1)
	*value = elm.Elm2.(T2)
	return true
}

// RegisterIter1 registers parameters from your DoFn with a
// signature func(*T) bool and optimizes their execution.
// This must be done by passing in a reference to an instantiated version
// of the function, aka:
// registration.RegisterIter1[T]((*func(*T) bool)(nil))
func RegisterIter1[T any](i *func(*T) bool) {
	registerFunc := func(s exec.ReStream) exec.ReusableInput {
		ret := &iterNative1[T]{s: s}
		ret.fn = ret.invoke
		return ret
	}
	exec.RegisterInput(reflect.TypeOf(i).Elem(), registerFunc)
}

// RegisterIter1 registers parameters from your DoFn with a
// signature func(*T1, *T2) bool and optimizes their execution.
// This must be done by passing in a reference to an instantiated version
// of the function, aka:
// registration.RegisterIter2[T1, T2]((*func(*T1, *T2) bool)(nil))
func RegisterIter2[T1, T2 any](i *func(*T1, *T2) bool) {
	registerFunc := func(s exec.ReStream) exec.ReusableInput {
		ret := &iterNative2[T1, T2]{s: s}
		ret.fn = ret.invoke
		return ret
	}
	exec.RegisterInput(reflect.TypeOf(i).Elem(), registerFunc)
}
