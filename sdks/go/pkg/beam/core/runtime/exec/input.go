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
	"sync"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

// TODO(herohde) 4/26/2017: SideInput representation? We want it to be amenable
// to the State API. For now, just use Stream.

// TODO(BEAM-3303): need to include windowing as well when resetting values.

// ReusableInput is a resettable value, notably used to unwind iterators cheaply
// and cache materialized side input across invocations.
type ReusableInput interface {
	// Init initializes the value before use.
	Init() error
	// Value returns the side input value.
	Value() interface{}
	// Reset resets the value after use.
	Reset() error
}

var (
	inputs   = make(map[reflect.Type]func(ReStream) ReusableInput)
	inputsMu sync.Mutex
)

// RegisterInput registers an input handler for the given type, such as "func(*int)bool". If
// multiple input handlers are registered for the same type, the last registration wins.
func RegisterInput(t reflect.Type, maker func(ReStream) ReusableInput) {
	inputsMu.Lock()
	defer inputsMu.Unlock()

	if _, exists := inputs[t]; exists {
		log.Warnf(context.Background(), "Input for %v already registered. Overwriting.", t.String())
	}
	inputs[t] = maker
}

type reIterValue struct {
	t  reflect.Type
	s  ReStream
	fn interface{}
}

func makeReIter(t reflect.Type, s ReStream) ReusableInput {
	if !funcx.IsReIter(t) {
		panic(fmt.Sprintf("illegal re-iter type: %v", t))
	}

	ret := &reIterValue{t: t, s: s}
	ret.fn = reflect.MakeFunc(t, ret.invoke).Interface()
	return ret
}

func (v *reIterValue) Init() error {
	return nil
}

func (v *reIterValue) Value() interface{} {
	return v.fn
}

func (v *reIterValue) Reset() error {
	return nil
}

func (v *reIterValue) invoke(args []reflect.Value) []reflect.Value {
	iter := makeIter(v.t.Out(0), v.s)
	iter.Init()
	return []reflect.Value{reflect.ValueOf(iter.Value())}
}

type iterValue struct {
	s     ReStream
	fn    interface{}
	types []reflect.Type

	// cur is the "current" stream, if any.
	cur Stream
}

func makeIter(t reflect.Type, s ReStream) ReusableInput {
	inputsMu.Lock()
	maker, exists := inputs[t]
	inputsMu.Unlock()

	if exists {
		return maker(s)
	}

	// If no specialized implementation is available, we use the (slower)
	// reflection-based one.

	types, ok := funcx.UnfoldIter(t)
	if !ok {
		panic(fmt.Sprintf("illegal iter type: %v", t))
	}

	ret := &iterValue{types: types, s: s}
	ret.fn = reflect.MakeFunc(t, ret.invoke).Interface()
	return ret
}

func (v *iterValue) Init() error {
	v.cur = v.s.Open()
	return nil
}

func (v *iterValue) Value() interface{} {
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
			v = reflect.ValueOf(Convert(elm.Elm, t))
			isKey = false
		default:
			v = reflect.ValueOf(Convert(elm.Elm2, t))
		}
		args[i].Elem().Set(v)
	}
	return []reflect.Value{reflect.ValueOf(true)}
}

type fixedValue struct {
	val interface{}
}

func (v *fixedValue) Init() error {
	return nil
}

func (v *fixedValue) Value() interface{} {
	return v.val
}

func (v *fixedValue) Reset() error {
	return nil
}
