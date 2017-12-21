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
	"io"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

// NOTE(herohde) 12/11/2017: the below helpers are ripe for type-specialization,
// if the reflection overhead here turns out to be significant. It would
// be nice to be able to quantify any potential improvements first, however.

// MainInput is the main input and is unfolded in the invocation, if present.
type MainInput struct {
	Key    FullValue
	Values []ReStream
}

// Invoke invokes the fn with the given values. The extra values must match the non-main
// side input and emitters. It returns the direct output, if any.
func Invoke(ctx context.Context, fn *funcx.Fn, opt *MainInput, extra ...reflect.Value) (*FullValue, error) {
	if fn == nil {
		return nil, nil // ok: nothing to Invoke
	}

	// (1) Populate contexts

	args := make([]reflect.Value, len(fn.Param))

	if index, ok := fn.Context(); ok {
		args[index] = reflect.ValueOf(ctx)
	}

	// (2) Main input from value, if any.

	in := fn.Params(funcx.FnValue | funcx.FnIter | funcx.FnReIter | funcx.FnEmit)
	i := 0

	if opt != nil {
		if index, ok := fn.EventTime(); ok {
			args[index] = reflect.ValueOf(opt.Key.Timestamp)
		}

		args[in[i]] = Convert(opt.Key.Elm, fn.Param[in[i]].T)
		i++
		if opt.Key.Elm2.Kind() != reflect.Invalid {
			args[in[i]] = Convert(opt.Key.Elm2, fn.Param[in[i]].T)
			i++
		}

		for _, iter := range opt.Values {
			param := fn.Param[in[i]]

			if param.Kind != funcx.FnIter {
				return nil, fmt.Errorf("GBK result values must be iterable: %v", param)
			}

			// TODO(herohde) 12/12/2017: allow form conversion on GBK results?

			args[in[i]] = makeIter(param.T, iter).Value()
			i++
		}
	}

	// (3) Precomputed side input and emitters (or other output).

	for _, arg := range extra {
		args[in[i]] = arg
		i++
	}

	// (4) Invoke

	ret, err := reflectCallNoPanic(fn.Fn, args)
	if err != nil {
		return nil, err
	}
	if index, ok := fn.Error(); ok && ret[index].Interface() != nil {
		return nil, ret[index].Interface().(error)
	}

	// (5) Return direct output, if any.

	out := fn.Returns(funcx.RetValue)
	if len(out) > 0 {
		value := &FullValue{}
		if index, ok := fn.OutEventTime(); ok {
			value.Timestamp = ret[index].Interface().(typex.EventTime)
		}

		value.Elm = ret[out[0]]
		if len(out) > 1 {
			value.Elm2 = ret[out[1]]
		}
		return value, nil
	}

	return nil, nil
}

func makeSideInputs(fn *funcx.Fn, in []*graph.Inbound, side []ReStream) ([]ReusableSideInput, error) {
	if len(side) == 0 {
		return nil, nil // ok: no side input
	}

	if len(in) != len(side)+1 {
		return nil, fmt.Errorf("found %v inbound, want %v", len(in), len(side)+1)
	}
	param := fn.Params(funcx.FnValue | funcx.FnIter | funcx.FnReIter)
	if len(param) <= len(side) {
		return nil, fmt.Errorf("found %v params, want >%v", len(param), len(side))
	}

	// The side input are last of the above params, so we can compute the offset easily.
	offset := len(param) - len(side)

	var ret []ReusableSideInput
	for i := 0; i < len(side); i++ {
		s, err := makeSideInput(in[i+1].Kind, fn.Param[i+offset].T, side[i])
		if err != nil {
			return nil, fmt.Errorf("failed to make side input %v: %v", i, err)
		}
		ret = append(ret, s)
	}
	return ret, nil
}

func makeEmitters(fn *funcx.Fn, nodes []Node) ([]ReusableEmitter, error) {
	if len(nodes) == 0 {
		return nil, nil // ok: no output nodes
	}

	offset := 0
	if len(fn.Returns(funcx.RetValue)) > 0 {
		offset = 1
	}
	out := fn.Params(funcx.FnEmit)
	if len(out) != len(nodes)-offset {
		return nil, fmt.Errorf("found %v emitters, want %v", len(out), len(nodes)-offset)
	}

	var ret []ReusableEmitter
	for i := 0; i < len(out); i++ {
		param := fn.Param[out[i]]
		ret = append(ret, makeEmit(param.T, nodes[i+offset]))
	}
	return ret, nil
}

// makeSideInput returns a reusable side input of the given kind and type.
func makeSideInput(kind graph.InputKind, t reflect.Type, values ReStream) (ReusableSideInput, error) {
	switch kind {
	case graph.Singleton:
		elms, err := ReadAll(values.Open())
		if err != nil {
			return nil, err
		}
		if len(elms) != 1 {
			return nil, fmt.Errorf("singleton side input %v for %v ill-defined", kind, t)
		}
		return &fixedValue{val: Convert(elms[0].Elm, t)}, nil

	case graph.Slice:
		elms, err := ReadAll(values.Open())
		if err != nil {
			return nil, err
		}
		slice := reflect.MakeSlice(t, len(elms), len(elms))
		for i := 0; i < len(elms); i++ {
			slice.Index(i).Set(Convert(elms[i].Elm, t.Elem()))
		}
		return &fixedValue{val: slice}, nil

	case graph.Iter:
		return makeIter(t, values), nil

	case graph.ReIter:
		return makeReIter(t, values), nil

	default:
		panic(fmt.Sprintf("Unexpected side input kind: %v", kind))
	}
}

// TODO(herohde) 4/26/2017: SideInput representation? We want it to be amenable
// to the State API. For now, just use Stream.

// TODO(BEAM-3303): need to include windowing as well when resetting values.

// ReusableSideInput is a resettable value, notably used to unwind iterators cheaply
// and cache materialized side input across invocations.
type ReusableSideInput interface {
	// Init initializes the value before use.
	Init() error
	// Value returns the side input value.
	Value() reflect.Value
	// Reset resets the value after use.
	Reset() error
}

type reIterValue struct {
	t  reflect.Type
	s  ReStream
	fn reflect.Value
}

func makeReIter(t reflect.Type, s ReStream) ReusableSideInput {
	if !funcx.IsReIter(t) {
		panic(fmt.Sprintf("illegal re-iter type: %v", t))
	}

	ret := &reIterValue{t: t, s: s}
	ret.fn = reflect.MakeFunc(t, ret.invoke)
	return ret
}

func (v *reIterValue) Init() error {
	return nil
}

func (v *reIterValue) Value() reflect.Value {
	return v.fn
}

func (v *reIterValue) Reset() error {
	return nil
}

func (v *reIterValue) invoke(args []reflect.Value) []reflect.Value {
	iter := makeIter(v.t.Out(0), v.s)
	iter.Init()
	return []reflect.Value{iter.Value()}
}

type iterValue struct {
	s     ReStream
	fn    reflect.Value
	types []reflect.Type

	// cur is the "current" stream, if any.
	cur Stream
}

func makeIter(t reflect.Type, s ReStream) ReusableSideInput {
	types, ok := funcx.UnfoldIter(t)
	if !ok {
		panic(fmt.Sprintf("illegal iter type: %v", t))
	}

	ret := &iterValue{types: types, s: s}
	ret.fn = reflect.MakeFunc(t, ret.invoke)
	return ret
}

func (v *iterValue) Init() error {
	v.cur = v.s.Open()
	return nil
}

func (v *iterValue) Value() reflect.Value {
	return v.fn
}

func (v *iterValue) Reset() error {
	if err := v.cur.Close(); err != nil {
		return err
	}
	v.cur = nil
	return nil
}

func (v *iterValue) invoke(args []reflect.Value) []reflect.Value {
	elm, err := v.cur.Read()
	if err != nil {
		if err == io.EOF {
			return []reflect.Value{reflect.ValueOf(false)}
		}
		panic(fmt.Sprintf("broken stream: %v", err))
	}

	// We expect 1-3 out parameters: func (*int, *string) bool.

	isKey := true
	for i, t := range v.types {
		var v reflect.Value
		switch {
		case t == typex.EventTimeType:
			v = reflect.ValueOf(elm.Timestamp)
		case isKey:
			v = Convert(elm.Elm, t)
			isKey = false
		default:
			v = Convert(elm.Elm2, t)
		}
		args[i].Elem().Set(v)
	}
	return []reflect.Value{reflect.ValueOf(true)}
}

type fixedValue struct {
	val reflect.Value
}

func (v *fixedValue) Init() error {
	return nil
}

func (v *fixedValue) Value() reflect.Value {
	return v.val
}

func (v *fixedValue) Reset() error {
	return nil
}

// ReusableEmitter is a resettable value needed to hold the implicit context and
// emit event time.
type ReusableEmitter interface {
	// Init resets the value. Can be called multiple times.
	Init(ctx context.Context, t typex.EventTime) error
	// Value returns the side input value. Contant value.
	Value() reflect.Value
}

type emitValue struct {
	n     Node
	fn    reflect.Value
	types []reflect.Type

	ctx context.Context
	et  typex.EventTime
}

func makeEmit(t reflect.Type, n Node) ReusableEmitter {
	types, ok := funcx.UnfoldEmit(t)
	if !ok {
		panic(fmt.Sprintf("illegal emit type: %v", t))
	}

	ret := &emitValue{n: n, types: types}
	ret.fn = reflect.MakeFunc(t, ret.invoke)
	return ret
}

func (e *emitValue) Init(ctx context.Context, et typex.EventTime) error {
	e.ctx = ctx
	e.et = et
	return nil
}

func (e *emitValue) Value() reflect.Value {
	return e.fn
}

func (e *emitValue) invoke(args []reflect.Value) []reflect.Value {
	value := FullValue{Timestamp: e.et}
	isKey := true
	for i, t := range e.types {
		switch {
		case t == typex.EventTimeType:
			value.Timestamp = args[i].Interface().(typex.EventTime)
		case isKey:
			value.Elm = args[i]
			isKey = false
		default:
			value.Elm2 = args[i]
		}
	}

	if err := e.n.ProcessElement(e.ctx, value); err != nil {
		// NOTE(herohde) 12/11/2017: emitters do not return an error, so if there
		// are problems we rely on the receiving node capturing the error.
		// Furthermore, we panic to quickly halt processing -- a corner-case
		// is that this panic unwinds _through_ user code and may be caught or
		// ignored, in which case we fall back failing bundle when the error is
		// returned by FinishBundle.

		panic(err)
	}
	return nil
}
