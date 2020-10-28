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
	"os"
	"reflect"
	"sync"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/symtab"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

var (
	// Resolver is the accessible symbol resolver the runtime uses to find functions.
	Resolver SymbolResolver
	cache    = make(map[string]interface{})
	mu       sync.Mutex
)

func init() {
	// defer initialization of the default resolver. This way
	// the symbol table isn't read in unless strictly necessary.
	Resolver = &deferedResolver{initFn: initResolver}
}

type deferedResolver struct {
	initFn func() SymbolResolver
	r      SymbolResolver
	init   sync.Once
}

func (d *deferedResolver) Sym2Addr(name string) (uintptr, error) {
	d.init.Do(func() {
		d.r = d.initFn()
	})
	return d.r.Sym2Addr(name)
}

func initResolver() SymbolResolver {
	// First try the Linux location, since it's the most reliable.
	if r, err := symtab.New("/proc/self/exe"); err == nil {
		return r
	}
	// For other OS's this works in most cases we need.
	if r, err := symtab.New(os.Args[0]); err == nil {
		return r
	}
	return failResolver(false)
}

// SymbolResolver resolves a symbol to an unsafe address.
type SymbolResolver interface {
	// Sym2Addr returns the address pointer for a given symbol.
	Sym2Addr(string) (uintptr, error)
}

// RegisterFunction allows function registration. It is beneficial for performance
// and is needed for functions -- such as custom coders -- serialized during unit
// tests, where the underlying symbol table is not available. It should be called
// in `init()` only.
func RegisterFunction(fn interface{}) {
	if initialized {
		panic("Init hooks have already run. Register function during init() instead.")
	}

	key := reflectx.FunctionName(fn)
	// If the function was registered already, the key and value will be the same anyway.
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
	return 0, errors.Errorf("%v not found. Use runtime.RegisterFunction in unit tests", name)
}
