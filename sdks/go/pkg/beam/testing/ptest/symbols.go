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

package ptest

import (
	"fmt"
	"reflect"
	rt "runtime"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
)

var symbolResolver = &resolver{registry: make(map[string]uintptr)}

func init() {
	// Setup manual symbol resolution for unit tests, where there dwarf
	// tables are not present. We automatically register the default
	// universal coders for convenience.
	runtime.SymbolResolver = symbolResolver

	RegisterFn(beam.JSONEnc)
	RegisterFn(beam.JSONDec)
}

type resolver struct {
	registry map[string]uintptr
}

// Register registers function for serialization in tests.
func (r *resolver) Register(fn interface{}) {
	val := reflect.ValueOf(fn)
	if val.Kind() != reflect.Func {
		panic(fmt.Sprintf("not a function: %v", val))
	}

	ptr := reflect.ValueOf(fn).Pointer()
	name := rt.FuncForPC(ptr).Name()
	r.registry[name] = ptr
}

func (r *resolver) Sym2Addr(name string) (uintptr, error) {
	ptr, ok := r.registry[name]
	if !ok {
		return 0, fmt.Errorf("%v not registered. Use ptest.RegisterFn in unit tests", name)
	}
	return ptr, nil
}

// RegisterFn is needed for functions serialized during unit tests, in
// particular custom coders.
func RegisterFn(fn interface{}) {
	symbolResolver.Register(fn)
}
