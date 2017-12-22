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

//go:generate specialize --input=decoders.tmpl --x=data,universals

// Decoder is a uniform custom encoder interface.
type Decoder interface {
	// Decode decodes the []byte in to a value of the given type.
	Decode(reflect.Type, []byte) (reflect.Value, error)
}

var (
	decoders   = make(map[string]func(reflect.Value) Decoder)
	decodersMu sync.Mutex
)

// RegisterDecoder registers an custom decoder invoker for the given type,
// such as "func(int)[]byte". If multiple decoder invokers are registered
// for the same type, the last registration wins.
func RegisterDecoder(t reflect.Type, maker func(reflect.Value) Decoder) {
	decodersMu.Lock()
	defer decodersMu.Unlock()

	decoders[t.String()] = maker
}

func makeDecoder(fn *funcx.Fn) Decoder {
	decodersMu.Lock()
	maker, exists := decoders[fn.Fn.Type().String()]
	decodersMu.Unlock()

	if exists {
		return maker(fn.Fn)
	}

	// If no specialized implementation is available, we use the (slower)
	// reflection-based one.

	return &decoder{fn: fn}
}

type decoder struct {
	fn *funcx.Fn
}

func (d *decoder) Decode(t reflect.Type, data []byte) (reflect.Value, error) {
	args := make([]reflect.Value, len(d.fn.Param))
	if index, ok := d.fn.Type(); ok {
		args[index] = reflect.ValueOf(t)
	}
	params := d.fn.Params(funcx.FnValue)
	args[params[0]] = reflect.ValueOf(data)

	ret, err := reflectCallNoPanic(d.fn.Fn, args)
	if err != nil {
		return reflect.Value{}, err
	}
	if index, ok := d.fn.Error(); ok && !ret[index].IsNil() {
		return reflect.Value{}, fmt.Errorf("decode error: %v", ret[index].Interface())
	}
	return ret[d.fn.Returns(funcx.RetValue)[0]], nil
}
