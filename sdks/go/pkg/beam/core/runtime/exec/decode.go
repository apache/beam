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

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

// Decoder is a uniform custom encoder interface. It wraps various
// forms of reflectx.Funcs.
type Decoder interface {
	// Decode decodes the []byte in to a value of the given type.
	Decode(reflect.Type, []byte) (interface{}, error)
}

func makeDecoder(fn reflectx.Func) Decoder {
	// Detect one of the valid decoder forms and allow it to be invoked
	// efficiently, relying on the general reflectx.Func framework for
	// avoiding expensive reflection calls. There are 4 forms:
	//
	//          []byte -> T
	//          []byte -> (T, error)
	//  (Type, []byte) -> T
	//  (Type, []byte) -> (T, error)
	//
	// We simply enumerate the forms. The wrappers simply ignore the
	// appropriate parts.

	switch fn.Type().NumIn() {
	case 1:
		switch fn.Type().NumOut() {
		case 1:
			return &decoder1x1{fn: reflectx.ToFunc1x1(fn)}
		case 2:
			return &decoder1x2{fn: reflectx.ToFunc1x2(fn)}
		}
	case 2:
		switch fn.Type().NumOut() {
		case 1:
			return &decoder2x1{fn: reflectx.ToFunc2x1(fn)}
		case 2:
			return &decoder2x2{fn: reflectx.ToFunc2x2(fn)}
		}
	}
	panic(fmt.Sprintf("Invalid decoder: %v", fn))
}

type decoder1x1 struct {
	fn reflectx.Func1x1
}

func (d *decoder1x1) Decode(t reflect.Type, data []byte) (interface{}, error) {
	return d.fn.Call1x1(data), nil
}

type decoder1x2 struct {
	fn reflectx.Func1x2
}

func (d *decoder1x2) Decode(t reflect.Type, data []byte) (interface{}, error) {
	val, err := d.fn.Call1x2(data)
	if err != nil {
		return nil, err.(error)
	}
	return val, nil
}

type decoder2x1 struct {
	fn reflectx.Func2x1
}

func (d *decoder2x1) Decode(t reflect.Type, data []byte) (interface{}, error) {
	return d.fn.Call2x1(t, data), nil
}

type decoder2x2 struct {
	fn reflectx.Func2x2
}

func (d *decoder2x2) Decode(t reflect.Type, data []byte) (interface{}, error) {
	val, err := d.fn.Call2x2(t, data)
	if err != nil {
		return nil, err.(error)
	}
	return val, nil
}
