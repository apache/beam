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

package reflectx

import (
	"reflect"
	"sync"

	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"runtime/debug"
)

//go:generate specialize --input=calls.tmpl
//go:generate gofmt -w calls.go

// Func represents a callable untyped function. This indirection allows
// us to avoid reflection call overhead for certain types as well as
// faster dynamic function implementations.
type Func interface {
	// Name returns the name of the function.
	Name() string
	// Type returns the type.
	Type() reflect.Type
	// Call invokes the implicit fn with arguments.
	Call(args []interface{}) []interface{}
}

var (
	funcs   = make(map[string]func(interface{}) Func)
	funcsMu sync.Mutex
)

// RegisterFunc registers an custom Func factory for the given type, such as
// "func(int)bool". If multiple Func factories are registered for the same type,
// the last registration wins.
func RegisterFunc(t reflect.Type, maker func(interface{}) Func) {
	funcsMu.Lock()
	defer funcsMu.Unlock()

	key := t.String()
	funcs[key] = maker
}

// MakeFunc returns a Func for given function.
func MakeFunc(fn interface{}) Func {
	funcsMu.Lock()
	maker, exists := funcs[reflect.TypeOf(fn).String()]
	funcsMu.Unlock()

	if exists {
		return maker(fn)
	}

	// If no specialized implementation is available, we use the standard
	// reflection-based call.

	return &reflectFunc{fn: reflect.ValueOf(fn)}
}

type reflectFunc struct {
	fn reflect.Value
}

func (c *reflectFunc) Name() string {
	return FunctionName(c.fn.Interface())
}

func (c *reflectFunc) Type() reflect.Type {
	return c.fn.Type()
}

func (c *reflectFunc) Call(args []interface{}) []interface{} {
	return Interface(c.fn.Call(ValueOf(args)))
}

// CallNoPanic calls the given Func and catches any panic.
func CallNoPanic(fn Func, args []interface{}) (ret []interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("panic: %v %s", r, debug.Stack())
		}
	}()
	return fn.Call(args), nil
}

// ValueOf performs a per-element reflect.ValueOf.
func ValueOf(list []interface{}) []reflect.Value {
	ret := make([]reflect.Value, len(list), len(list))
	for i := 0; i < len(list); i++ {
		ret[i] = reflect.ValueOf(list[i])
	}
	return ret
}

// Interface performs a per-element Interface call.
func Interface(list []reflect.Value) []interface{} {
	ret := make([]interface{}, len(list), len(list))
	for i := 0; i < len(list); i++ {
		ret[i] = list[i].Interface()
	}
	return ret
}
