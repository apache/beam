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
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

// windowFnMeta holds validated metadata about a registered custom WindowFn type.
type windowFnMeta struct {
	// Type is the struct type (with pointer stripped), e.g. myWindowFn.
	Type reflect.Type
	// ElemType is the concrete element parameter type, or nil for
	// timestamp-only signatures.
	ElemType reflect.Type
}

// NeedsElement reports whether this WindowFn signature accepts an element.
func (m *windowFnMeta) NeedsElement() bool {
	return m.ElemType != nil
}

var (
	windowFnRegistryMu sync.RWMutex
	windowFnRegistry   = map[reflect.Type]*windowFnMeta{}
)

// LookupWindowFnMeta returns the validated metadata for a registered custom
// WindowFn type. The key is the struct type (pointer stripped).
// Returns nil if not registered.
func LookupWindowFnMeta(t reflect.Type) *windowFnMeta {
	windowFnRegistryMu.RLock()
	defer windowFnRegistryMu.RUnlock()
	return windowFnRegistry[t]
}

var (
	eventTimeType   = reflect.TypeFor[typex.EventTime]()
	windowSliceType = reflect.TypeFor[[]typex.Window]()
)

// RegisterWindowFn registers a custom WindowFn type so it can be serialized
// and deserialized across process boundaries. Call RegisterWindowFn during
// init for every custom window function type used in the pipeline.
//
// The type parameter T must be a pointer-to-struct type with an
// AssignWindows method of one of the following shapes:
//
//	func (f *MyFn) AssignWindows(ts typex.EventTime) []typex.Window
//	func (f *MyFn) AssignWindows(ts typex.EventTime, elem T) []typex.Window
//
// RegisterWindowFn panics if the type is invalid or already registered.
//
// Example:
//
//	func init() {
//	    window.RegisterWindowFn[*myWindowFn]()
//	}
func RegisterWindowFn[T any]() {
	var v T
	t := reflect.TypeOf(v)
	if t == nil {
		panic("window.RegisterWindowFn: T must not be an untyped nil interface")
	}
	if t.Kind() != reflect.Pointer || t.Elem().Kind() != reflect.Struct {
		panic(fmt.Sprintf("window.RegisterWindowFn: T must be a pointer to struct, got %v", t))
	}

	structType := t.Elem()

	m, ok := t.MethodByName("AssignWindows")
	if !ok {
		panic(fmt.Sprintf("window.RegisterWindowFn: %v has no AssignWindows method", t))
	}

	meta := validateAssignWindows(t, m)

	windowFnRegistryMu.Lock()
	defer windowFnRegistryMu.Unlock()

	if _, dup := windowFnRegistry[structType]; dup {
		panic(fmt.Sprintf("window.RegisterWindowFn: %v is already registered", t))
	}
	windowFnRegistry[structType] = meta

	runtime.RegisterType(reflect.TypeOf(v))
}

// validateAssignWindows checks that the method has a valid signature and
// returns the corresponding metadata.
func validateAssignWindows(ptrType reflect.Type, m reflect.Method) *windowFnMeta {
	mt := m.Type
	// Method type includes the receiver as first param.
	// Valid shapes:
	//   (receiver, typex.EventTime) -> []typex.Window                   numIn=2
	//   (receiver, typex.EventTime, elemType) -> []typex.Window         numIn=3

	if mt.NumOut() != 1 || mt.Out(0) != windowSliceType {
		panic(fmt.Sprintf(
			"window.RegisterWindowFn: %v.AssignWindows must return []typex.Window, got %v",
			ptrType, mt))
	}

	switch mt.NumIn() {
	case 2:
		// (receiver, typex.EventTime)
		if mt.In(1) != eventTimeType {
			panic(fmt.Sprintf(
				"window.RegisterWindowFn: %v.AssignWindows first param must be typex.EventTime, got %v",
				ptrType, mt.In(1)))
		}
		return &windowFnMeta{Type: ptrType.Elem(), ElemType: nil}

	case 3:
		// (receiver, typex.EventTime, elemType)
		if mt.In(1) != eventTimeType {
			panic(fmt.Sprintf(
				"window.RegisterWindowFn: %v.AssignWindows first param must be typex.EventTime, got %v",
				ptrType, mt.In(1)))
		}
		elemType := mt.In(2)
		return &windowFnMeta{Type: ptrType.Elem(), ElemType: elemType}

	default:
		panic(fmt.Sprintf(
			"window.RegisterWindowFn: %v.AssignWindows must take (typex.EventTime) or (typex.EventTime, T), got %d params (excluding receiver)",
			ptrType, mt.NumIn()-1))
	}
}
