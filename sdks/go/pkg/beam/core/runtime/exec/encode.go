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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

// Encoder is a uniform custom encoder interface. It wraps various
// forms of reflectx.Funcs.
type Encoder interface {
	// Encode encodes the given value (of the given type).
	Encode(reflect.Type, any) ([]byte, error)
}

func makeEncoder(fn reflectx.Func) Encoder {
	// Detect one of the valid encoder forms and allow it to be invoked
	// efficiently, relying on the general reflectx.Func framework for
	// avoiding expensive reflection calls. There are 4 forms:
	//
	//          T -> []byte
	//          T -> ([]byte, error)
	//  (Type, T) -> []byte
	//  (Type, T) -> ([][byte, error)
	//
	// We simply enumerate the forms. The wrappers simply ignore the
	// appropriate parts.

	switch fn.Type().NumIn() {
	case 1:
		switch fn.Type().NumOut() {
		case 1:
			return &encoder1x1{fn: reflectx.ToFunc1x1(fn)}
		case 2:
			return &encoder1x2{fn: reflectx.ToFunc1x2(fn)}
		}
	case 2:
		switch fn.Type().NumOut() {
		case 1:
			return &encoder2x1{fn: reflectx.ToFunc2x1(fn)}
		case 2:
			return &encoder2x2{fn: reflectx.ToFunc2x2(fn)}
		}
	}
	panic(fmt.Sprintf("Invalid encoder: %v", fn))
}

type encoder1x1 struct {
	fn reflectx.Func1x1
}

func (d *encoder1x1) Encode(t reflect.Type, v any) ([]byte, error) {
	return d.fn.Call1x1(v).([]byte), nil
}

type encoder1x2 struct {
	fn reflectx.Func1x2
}

func (d *encoder1x2) Encode(t reflect.Type, v any) ([]byte, error) {
	val, err := d.fn.Call1x2(v)
	if err != nil {
		return nil, err.(error)
	}
	return val.([]byte), nil
}

type encoder2x1 struct {
	fn reflectx.Func2x1
}

func (d *encoder2x1) Encode(t reflect.Type, v any) ([]byte, error) {
	return d.fn.Call2x1(t, v).([]byte), nil
}

type encoder2x2 struct {
	fn reflectx.Func2x2
}

func (d *encoder2x2) Encode(t reflect.Type, v any) ([]byte, error) {
	val, err := d.fn.Call2x2(t, v)
	if err != nil {
		return nil, err.(error)
	}
	return val.([]byte), nil
}
