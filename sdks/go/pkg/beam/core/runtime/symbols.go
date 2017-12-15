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
	"sync"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/symtab"
)

// TODO(wcn): not happy with these names. if inspiration strikes, changes welcome!

// SymbolResolution is the interface that should be implemented
// in order to override the default behavior for symbol resolution.
type SymbolResolution interface {
	// Sym2Addr returns the address pointer for a given symbol.
	Sym2Addr(string) (uintptr, error)
}

// SymbolResolver is exported to allow unit tests to supply a resolver function.
// This is needed since the default symbol resolution process uses the DWARF
// tables in the executable, which unfortunately are not made available
// by default in "go test" builds. Unit tests that need symbol resolution
// should pass in an appropriate fake.
var SymbolResolver SymbolResolution

func init() {
	// First try the Linux location, since it's the most reliable.
	if r, err := symtab.New("/proc/self/exe"); err == nil {
		SymbolResolver = NewSymbolCache(r)
		return
	}
	// For other OS's this works in most cases we need. If it doesn't, log
	// an error and keep going.
	if r, err := symtab.New(os.Args[0]); err == nil {
		SymbolResolver = NewSymbolCache(r)
		return
	}
	SymbolResolver = panicResolver(false)
}

// SymbolCache added a caching layer over any SymbolResolution.
type SymbolCache struct {
	resolver SymbolResolution
	cache    map[string]uintptr
	mu       sync.Mutex
}

// NewSymbolCache adds caching to the given symbol resolver.
func NewSymbolCache(r SymbolResolution) SymbolResolution {
	return &SymbolCache{resolver: r, cache: make(map[string]uintptr)}
}

func (c *SymbolCache) Sym2Addr(sym string) (uintptr, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if p, exists := c.cache[sym]; exists {
		return p, nil
	}

	p, err := c.resolver.Sym2Addr(sym)
	if err != nil {
		return 0, err
	}

	c.cache[sym] = p
	return p, nil
}

type panicResolver bool

func (p panicResolver) Sym2Addr(name string) (uintptr, error) {
	panic("Attempted call to panicResolver.Sym2Addr(): if this happened from a unit test, you need to supply a fake symbol resolver.")
}
