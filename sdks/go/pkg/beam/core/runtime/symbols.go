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

package runtime

import (
	"fmt"
	"os"
	"reflect"
	"sync"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/symtab"
)

var (
	Resolver SymbolResolver
	cache    = make(map[string]interface{})
	mu       sync.Mutex
)

func init() {
	// First try the Linux location, since it's the most reliable.
	if r, err := symtab.New("/proc/self/exe"); err == nil {
		Resolver = r
		return
	}
	// For other OS's this works in most cases we need.
	if r, err := symtab.New(os.Args[0]); err == nil {
		Resolver = r
		return
	}
	Resolver = failResolver(false)
}

// SymbolResolver resolves a symbol to an unsafe address.
type SymbolResolver interface {
	// Sym2Addr returns the address pointer for a given symbol.
	Sym2Addr(string) (uintptr, error)
}

// RegisterFunction allows function registration. It is beneficial for performance
// and is needed for functions -- such as custom coders -- serialized during unit
// tests, where the underlying symbol table is not available. It should be called
// in init() only. Returns the external key for the function.
func RegisterFunction(fn interface{}) {
	if initialized {
		panic("Init hooks have already run. Register function during init() instead.")
	}

	key := reflectx.FunctionName(fn)
	if _, exists := cache[key]; exists {
		panic(fmt.Sprintf("Function %v already registred", key))
	}
	cache[key] = fn
}

// ResolveFunction resolves the runtime value of a given function by symbol name
// and type.
func ResolveFunction(name string, t reflect.Type) (interface{}, error) {
	mu.Lock()
	defer mu.Unlock()

	if val, exists := cache[name]; exists {
		return val, nil
	}

	ptr, err := Resolver.Sym2Addr(name)
	if err != nil {
		return 0, err
	}
	val := reflectx.LoadFunction(ptr, t)
	cache[name] = val
	return val, nil
}

type failResolver bool

func (p failResolver) Sym2Addr(name string) (uintptr, error) {
	return 0, fmt.Errorf("%v not found. Use runtime.RegisterFunction in unit tests", name)
}
