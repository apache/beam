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
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

// tsOnlyAssigner is the fast-path interface for timestamp-only custom WindowFns.
type tsOnlyAssigner interface {
	AssignWindows(typex.EventTime) []typex.Window
}

// anyElemAssigner is the fast-path interface for element-aware WindowFns whose
// element parameter is typed as any.
type anyElemAssigner interface {
	AssignWindows(typex.EventTime, any) []typex.Window
}

// anyKVAssigner is the fast-path interface for KV-aware WindowFns whose key and
// value parameters are both typed as any.
type anyKVAssigner interface {
	AssignWindows(typex.EventTime, any, any) []typex.Window
}

// WindowFnInvoker wraps a custom WindowFn instance and dispatches AssignWindows
// through a path chosen once at construction: a typed interface assertion where
// the signature permits one, otherwise reflect.Value.Call, which costs a small
// per-call allocation.
//
// Element parameters follow the convention the DoFn invoker uses for main
// input: a KV element arrives as a key and a value, anything else as a single
// element.
type WindowFnInvoker struct {
	call func(ts typex.EventTime, elm, elm2 any) []typex.Window
}

// NewWindowFnInvoker builds an invoker for fn. The concrete type of fn must
// have been previously registered via RegisterWindowFn.
func NewWindowFnInvoker(fn any) (*WindowFnInvoker, error) {
	t := reflect.TypeOf(fn)
	if t == nil {
		return nil, errors.New("window.NewWindowFnInvoker: fn must not be nil")
	}
	structType := t
	if t.Kind() == reflect.Pointer {
		structType = t.Elem()
	}

	elems, ok := LookupWindowFn(structType)
	if !ok {
		return nil, errors.Errorf("window.NewWindowFnInvoker: type %v is not registered; call window.RegisterWindowFn during init()", t)
	}

	inv := &WindowFnInvoker{}

	switch len(elems) {
	case 0:
		if a, ok := fn.(tsOnlyAssigner); ok {
			inv.call = func(ts typex.EventTime, _, _ any) []typex.Window {
				return a.AssignWindows(ts)
			}
			return inv, nil
		}
	case 1:
		if a, ok := fn.(anyElemAssigner); ok {
			inv.call = func(ts typex.EventTime, elm, _ any) []typex.Window {
				return a.AssignWindows(ts, elm)
			}
			return inv, nil
		}
	default:
		if a, ok := fn.(anyKVAssigner); ok {
			inv.call = func(ts typex.EventTime, elm, elm2 any) []typex.Window {
				return a.AssignWindows(ts, elm, elm2)
			}
			return inv, nil
		}
	}

	// Concrete element types cannot be reached through an interface assertion.
	m := reflect.ValueOf(fn).MethodByName("AssignWindows")
	if !m.IsValid() {
		return nil, errors.Errorf("window.NewWindowFnInvoker: %v has no AssignWindows method", t)
	}

	switch len(elems) {
	case 0:
		inv.call = func(ts typex.EventTime, _, _ any) []typex.Window {
			out := m.Call([]reflect.Value{reflect.ValueOf(ts)})
			return out[0].Interface().([]typex.Window)
		}
	case 1:
		inv.call = func(ts typex.EventTime, elm, _ any) []typex.Window {
			out := m.Call([]reflect.Value{reflect.ValueOf(ts), reflect.ValueOf(elm)})
			return out[0].Interface().([]typex.Window)
		}
	default:
		inv.call = func(ts typex.EventTime, elm, elm2 any) []typex.Window {
			out := m.Call([]reflect.Value{reflect.ValueOf(ts), reflect.ValueOf(elm), reflect.ValueOf(elm2)})
			return out[0].Interface().([]typex.Window)
		}
	}
	return inv, nil
}

// Invoke calls AssignWindows on the underlying WindowFn. Signatures that do not
// take element parameters ignore elm and elm2; elm2 carries the value of a KV
// element.
func (inv *WindowFnInvoker) Invoke(ts typex.EventTime, elm, elm2 any) []typex.Window {
	return inv.call(ts, elm, elm2)
}
