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

package window

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

// tsOnlyAssigner is the fast-path interface for timestamp-only custom WindowFns.
type tsOnlyAssigner interface {
	AssignWindows(typex.EventTime) []typex.Window
}

// anyElemAssigner is the fast-path interface for element-aware WindowFns
// whose element parameter is typed as any (interface{}).
type anyElemAssigner interface {
	AssignWindows(typex.EventTime, any) []typex.Window
}

// WindowFnInvoker wraps a custom WindowFn instance and dispatches
// AssignWindows calls through one of three paths, chosen once at
// construction:
//
//  1. Type-assert to tsOnlyAssigner — zero allocation.
//  2. Type-assert to anyElemAssigner — zero allocation.
//  3. reflect.Value.Method.Call — small per-call allocation.
type WindowFnInvoker struct {
	call         func(typex.EventTime, any) []typex.Window
	needsElement bool
}

// NewWindowFnInvoker builds an invoker for fn. The concrete type of fn
// must have been previously registered via RegisterWindowFn.
// Panics if fn's type is not registered.
func NewWindowFnInvoker(fn any) *WindowFnInvoker {
	t := reflect.TypeOf(fn)
	if t == nil {
		panic("window.NewWindowFnInvoker: fn must not be nil")
	}
	structType := t
	if t.Kind() == reflect.Pointer {
		structType = t.Elem()
	}

	meta := LookupWindowFnMeta(structType)
	if meta == nil {
		panic(fmt.Sprintf("window.NewWindowFnInvoker: type %v is not registered; call window.RegisterWindowFn during init()", t))
	}

	inv := &WindowFnInvoker{needsElement: meta.NeedsElement()}

	if !meta.NeedsElement() {
		// Fast path 1: timestamp-only.
		if a, ok := fn.(tsOnlyAssigner); ok {
			inv.call = func(ts typex.EventTime, _ any) []typex.Window {
				return a.AssignWindows(ts)
			}
			return inv
		}
		// Unreachable for well-typed registrations, but fall through to reflect.
	} else {
		// Fast path 2: element typed as any.
		if a, ok := fn.(anyElemAssigner); ok {
			inv.call = func(ts typex.EventTime, elem any) []typex.Window {
				return a.AssignWindows(ts, elem)
			}
			return inv
		}
	}

	// Slow path 3: concrete element type — use reflect.
	rv := reflect.ValueOf(fn)
	m := rv.MethodByName("AssignWindows")
	if !m.IsValid() {
		panic(fmt.Sprintf("window.NewWindowFnInvoker: %v has no AssignWindows method", t))
	}

	if !meta.NeedsElement() {
		inv.call = func(ts typex.EventTime, _ any) []typex.Window {
			out := m.Call([]reflect.Value{reflect.ValueOf(ts)})
			return out[0].Interface().([]typex.Window)
		}
	} else {
		inv.call = func(ts typex.EventTime, elem any) []typex.Window {
			out := m.Call([]reflect.Value{reflect.ValueOf(ts), reflect.ValueOf(elem)})
			return out[0].Interface().([]typex.Window)
		}
	}
	return inv
}

// Invoke calls AssignWindows on the underlying WindowFn.
// If the WindowFn is timestamp-only, elem is ignored.
func (inv *WindowFnInvoker) Invoke(ts typex.EventTime, elem any) []typex.Window {
	return inv.call(ts, elem)
}

// NeedsElement reports whether the underlying WindowFn accepts an element.
func (inv *WindowFnInvoker) NeedsElement() bool {
	return inv.needsElement
}
