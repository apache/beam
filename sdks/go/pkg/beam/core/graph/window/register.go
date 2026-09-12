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

// windowFnRegistry maps the struct type of a registered custom WindowFn, with
// the pointer stripped, to the element parameter types of its AssignWindows
// method. Presence in the map rather than the value is what says a type is
// registered.
var (
	windowFnRegistryMu sync.RWMutex
	windowFnRegistry   = map[reflect.Type][]reflect.Type{}
)

// LookupWindowFn reports whether t is a registered custom WindowFn struct type
// and returns the element parameter types of its AssignWindows method. elems is
// empty for a timestamp-only signature, holds one type for a single element,
// and two for a KV element taken as a key and a value.
func LookupWindowFn(t reflect.Type) (elems []reflect.Type, ok bool) {
	windowFnRegistryMu.RLock()
	defer windowFnRegistryMu.RUnlock()
	elems, ok = windowFnRegistry[t]
	return elems, ok
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
//	func (f *MyFn) AssignWindows(ts typex.EventTime, k K, v V) []typex.Window
//
// The element parameters mirror how a DoFn receives its main input: a KV
// PCollection arrives as two parameters, anything else as one.
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

	elems := validateAssignWindows(t, m)

	windowFnRegistryMu.Lock()
	defer windowFnRegistryMu.Unlock()

	if _, dup := windowFnRegistry[structType]; dup {
		panic(fmt.Sprintf("window.RegisterWindowFn: %v is already registered", t))
	}
	windowFnRegistry[structType] = elems

	runtime.RegisterType(reflect.TypeOf(v))
}

// validateAssignWindows checks that the method has a valid signature and
// returns its element parameter types, empty if it takes only a timestamp.
func validateAssignWindows(ptrType reflect.Type, m reflect.Method) []reflect.Type {
	// The method type counts the receiver, so NumIn is one more than the
	// parameter list the user wrote: 2, 3 and 4 are the accepted shapes.
	mt := m.Type

	if mt.NumOut() != 1 || mt.Out(0) != windowSliceType {
		panic(fmt.Sprintf(
			"window.RegisterWindowFn: %v.AssignWindows must return []typex.Window, got %v",
			ptrType, mt))
	}
	if mt.NumIn() < 2 || mt.NumIn() > 4 {
		panic(fmt.Sprintf(
			"window.RegisterWindowFn: %v.AssignWindows must take (typex.EventTime), (typex.EventTime, T) or (typex.EventTime, K, V), got %d params (excluding receiver)",
			ptrType, mt.NumIn()-1))
	}
	if mt.In(1) != eventTimeType {
		panic(fmt.Sprintf(
			"window.RegisterWindowFn: %v.AssignWindows first param must be typex.EventTime, got %v",
			ptrType, mt.In(1)))
	}

	var elems []reflect.Type
	for i := 2; i < mt.NumIn(); i++ {
		elems = append(elems, mt.In(i))
	}
	return elems
}
