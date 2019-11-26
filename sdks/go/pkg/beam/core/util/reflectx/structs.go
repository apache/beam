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
	"context"
	"reflect"
	"sync"

	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

var (
	structFuncs   = make(map[string]func(interface{}) map[string]Func)
	structFuncsMu sync.Mutex
)

// RegisterStructWrapper takes in the reflect.Type of a structural DoFn, and
// a wrapping function that will take an instance of that struct type and
// produce a map of method names to of closured Funcs that call the method
// on the instance of the struct.
//
// The goal is to avoid the implicit reflective method invocation penalty
// that occurs when passing a method through the reflect package.
func RegisterStructWrapper(t reflect.Type, wrapper func(interface{}) map[string]Func) {
	structFuncsMu.Lock()
	defer structFuncsMu.Unlock()

	if t.Kind() != reflect.Struct {
		log.Fatalf(context.Background(), "RegisterStructWrapper for %v should be a struct type, but was %v", t, t.Kind())
	}

	key := t.String()
	if _, exists := structFuncs[key]; exists {
		log.Warnf(context.Background(), "StructWrapper for %v already registered. Overwriting.", key)
	}
	structFuncs[key] = wrapper
}

// WrapMethods takes in a struct value as an interface, and returns a map of
// method names to Funcs of those methods wrapped in a closure for the struct instance.
func WrapMethods(fn interface{}) (map[string]Func, bool) {
	return wrapMethodsKeyed(reflect.TypeOf(fn), fn)
}

// WrapMethodsKeyed takes in a struct value as an interface
func wrapMethodsKeyed(t reflect.Type, fn interface{}) (map[string]Func, bool) {
	structFuncsMu.Lock()
	defer structFuncsMu.Unlock()
	// Registering happens on the value, not the proto type.
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	key := t.String()
	if f, exists := structFuncs[key]; exists {
		return f(fn), true
	}
	return nil, false
}
