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
	"fmt"
	"reflect"
	"sync"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
)

//go:generate specialize --input=encoders.tmpl --x=data,universals

// Encoder is a uniform custom encoder interface.
type Encoder interface {
	// Encode encodes the given value (of the given type).
	Encode(reflect.Type, reflect.Value) ([]byte, error)
}

var (
	encoders   = make(map[string]func(reflect.Value) Encoder)
	encodersMu sync.Mutex
)

// RegisterEncoder registers an custom encoder invoker for the given type,
// such as "func(int)[]byte". If multiple encoder invokers are registered
// for the same type, the last registration wins.
func RegisterEncoder(t reflect.Type, maker func(reflect.Value) Encoder) {
	encodersMu.Lock()
	defer encodersMu.Unlock()

	encoders[t.String()] = maker
}

func makeEncoder(fn *funcx.Fn) Encoder {
	encodersMu.Lock()
	maker, exists := encoders[fn.Fn.Type().String()]
	encodersMu.Unlock()

	if exists {
		return maker(fn.Fn)
	}

	// If no specialized implementation is available, we use the (slower)
	// reflection-based one.

	return &encoder{fn: fn}
}

type encoder struct {
	fn *funcx.Fn
}

func (e *encoder) Encode(t reflect.Type, elm reflect.Value) ([]byte, error) {
	args := make([]reflect.Value, len(e.fn.Param))
	if index, ok := e.fn.Type(); ok {
		args[index] = reflect.ValueOf(t)
	}
	params := e.fn.Params(funcx.FnValue)
	args[params[0]] = elm

	ret, err := reflectCallNoPanic(e.fn.Fn, args)
	if err != nil {
		return nil, err
	}
	if index, ok := e.fn.Error(); ok && !ret[index].IsNil() {
		return nil, fmt.Errorf("encode error: %v", ret[index].Interface())
	}
	data := ret[e.fn.Returns(funcx.RetValue)[0]].Interface().([]byte)
	return data, nil
}
