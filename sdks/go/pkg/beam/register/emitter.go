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

package register

import (
	"context"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

type emit struct {
	est *sdf.WatermarkEstimator

	ctx   context.Context
	ws    []typex.Window
	et    typex.EventTime
	value exec.FullValue
}

func (e *emit) Init(ctx context.Context, ws []typex.Window, et typex.EventTime) error {
	e.ctx = ctx
	e.ws = ws
	e.et = et
	return nil
}

func (e *emit) AttachEstimator(est *sdf.WatermarkEstimator) {
	e.est = est
}

type emit1[T any] struct {
	emit
	n exec.ElementProcessor
}

func (e *emit1[T]) Value() any {
	return e.invoke
}

func (e *emit1[T]) invoke(val T) {
	e.value = exec.FullValue{Windows: e.ws, Timestamp: e.et, Elm: val}
	if e.est != nil {
		(*e.est).(sdf.TimestampObservingEstimator).ObserveTimestamp(e.et.ToTime())
	}
	if err := e.n.ProcessElement(e.ctx, &e.value); err != nil {
		panic(err)
	}
}

type emit2[T1, T2 any] struct {
	emit
	n exec.ElementProcessor
}

func (e *emit2[T1, T2]) Value() any {
	return e.invoke
}

func (e *emit2[T1, T2]) invoke(key T1, val T2) {
	e.value = exec.FullValue{Windows: e.ws, Timestamp: e.et, Elm: key, Elm2: val}
	if e.est != nil {
		(*e.est).(sdf.TimestampObservingEstimator).ObserveTimestamp(e.et.ToTime())
	}
	if err := e.n.ProcessElement(e.ctx, &e.value); err != nil {
		panic(err)
	}
}

type emit1WithTimestamp[T any] struct {
	emit
	n exec.ElementProcessor
}

func (e *emit1WithTimestamp[T]) Value() any {
	return e.invoke
}

func (e *emit1WithTimestamp[T]) invoke(et typex.EventTime, val T) {
	e.value = exec.FullValue{Windows: e.ws, Timestamp: et, Elm: val}
	if e.est != nil {
		(*e.est).(sdf.TimestampObservingEstimator).ObserveTimestamp(et.ToTime())
	}
	if err := e.n.ProcessElement(e.ctx, &e.value); err != nil {
		panic(err)
	}
}

type emit2WithTimestamp[T1, T2 any] struct {
	emit
	n exec.ElementProcessor
}

func (e *emit2WithTimestamp[T1, T2]) Value() any {
	return e.invoke
}

func (e *emit2WithTimestamp[T1, T2]) invoke(et typex.EventTime, key T1, val T2) {
	e.value = exec.FullValue{Windows: e.ws, Timestamp: et, Elm: key, Elm2: val}
	if e.est != nil {
		(*e.est).(sdf.TimestampObservingEstimator).ObserveTimestamp(et.ToTime())
	}
	if err := e.n.ProcessElement(e.ctx, &e.value); err != nil {
		panic(err)
	}
}

// Emitter1 registers parameters from your DoFn with a
// signature func(T) and optimizes their execution.
// This must be done by passing in type parameters of your input as a constraint,
// aka: register.Emitter1[T]()
func Emitter1[T1 any]() {
	e := (*func(T1))(nil)
	registerFunc := func(n exec.ElementProcessor) exec.ReusableEmitter {
		return &emit1[T1]{n: n}
	}
	eT := reflect.TypeOf(e).Elem()
	registerType(eT.In(0))
	exec.RegisterEmitter(eT, registerFunc)
}

// Emitter2 registers parameters from your DoFn with a
// signature func(T1, T2) and optimizes their execution.
// This must be done by passing in type parameters of all inputs (including EventTime)
// as constraints, aka: register.Emitter2[T1, T2](), where T2 is the type of your
// value and T2 is either the type of your key or the eventTime.
func Emitter2[T1, T2 any]() {
	e := (*func(T1, T2))(nil)
	registerFunc := func(n exec.ElementProcessor) exec.ReusableEmitter {
		return &emit2[T1, T2]{n: n}
	}
	if reflect.TypeOf(e).Elem().In(0) == typex.EventTimeType {
		registerFunc = func(n exec.ElementProcessor) exec.ReusableEmitter {
			return &emit1WithTimestamp[T2]{n: n}
		}
	}
	eT := reflect.TypeOf(e).Elem()
	registerType(eT.In(0))
	registerType(eT.In(1))
	exec.RegisterEmitter(eT, registerFunc)
}

// Emitter3 registers parameters from your DoFn with a
// signature func(beam.EventTime, T2, T3) and optimizes their execution.
// This must be done by passing in type parameters of all inputs as constraints,
// aka: register.Emitter3[beam.EventTime, T1, T2](), where T1 is the type of
// your key and T2 is the type of your value.
func Emitter3[ET typex.EventTime, T1, T2 any]() {
	e := (*func(ET, T1, T2))(nil)
	registerFunc := func(n exec.ElementProcessor) exec.ReusableEmitter {
		return &emit2WithTimestamp[T1, T2]{n: n}
	}
	eT := reflect.TypeOf(e).Elem()
	// No need to register event time.
	registerType(eT.In(1))
	registerType(eT.In(2))
	exec.RegisterEmitter(eT, registerFunc)
}
