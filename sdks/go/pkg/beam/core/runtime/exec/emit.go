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
	"sync"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

// ReusableEmitter is a resettable value needed to hold the implicit context and
// emit event time.
type ReusableEmitter interface {
	// Init resets the value. Can be called multiple times.
	Init(ctx context.Context, ws []typex.Window, t typex.EventTime) error
	// Value returns the side input value. Constant value.
	Value() interface{}
}

var (
	emitters   = make(map[string]func(ElementProcessor) ReusableEmitter)
	emittersMu sync.Mutex
)

// RegisterEmitter registers an emitter for the given type, such as "func(int)". If
// multiple emitters are registered for the same type, the last registration wins.
func RegisterEmitter(t reflect.Type, maker func(ElementProcessor) ReusableEmitter) {
	emittersMu.Lock()
	defer emittersMu.Unlock()

	key := t.String()
	emitters[key] = maker
}

// IsEmitterRegistered returns whether an emitter maker has already been registered.
func IsEmitterRegistered(t reflect.Type) bool {
	_, exists := emitters[t.String()]
	return exists
}

func makeEmit(t reflect.Type, n ElementProcessor) ReusableEmitter {
	emittersMu.Lock()
	maker, exists := emitters[t.String()]
	emittersMu.Unlock()

	if exists {
		return maker(n)
	}

	// If no specialized implementation is available, we use the (slower)
	// reflection-based one.

	types, ok := funcx.UnfoldEmit(t)
	if !ok {
		panic(fmt.Sprintf("illegal emit type: %v", t))
	}

	ret := &emitValue{n: n, types: types}
	ret.fn = reflect.MakeFunc(t, ret.invoke).Interface()
	return ret
}

// TODO(herohde) 1/19/2018: we could have an emitter for each arity in the reflection case.

// emitValue is the reflection-based default emitter implementation.
type emitValue struct {
	n     ElementProcessor
	fn    interface{}
	types []reflect.Type

	ctx context.Context
	ws  []typex.Window
	et  typex.EventTime
}

func (e *emitValue) Init(ctx context.Context, ws []typex.Window, et typex.EventTime) error {
	e.ctx = ctx
	e.ws = ws
	e.et = et
	return nil
}

func (e *emitValue) Value() interface{} {
	return e.fn
}

func (e *emitValue) invoke(args []reflect.Value) []reflect.Value {
	value := &FullValue{Windows: e.ws, Timestamp: e.et}
	isKey := true
	for i, t := range e.types {
		switch {
		case t == typex.EventTimeType:
			value.Timestamp = args[i].Interface().(typex.EventTime)
		case isKey:
			value.Elm = args[i].Interface()
			isKey = false
		default:
			value.Elm2 = args[i].Interface()
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
