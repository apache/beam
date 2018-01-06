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

//go:generate specialize --input=calls.tmpl

// Caller is an untyped function call interface. This indirection allows
// us to avoid reflection call overhead for certain types.
type Caller interface {
	// Type returns the type.
	Type() reflect.Type
	// Call invokes the implicit fn with arguments.
	Call(args []interface{}) []interface{}
}

var (
	callers   = make(map[string]func(interface{}) Caller)
	callersMu sync.Mutex
)

// RegisterCaller registers an custom caller factory for the given type, such as
// "func(int)bool". If multiple caller factories are registered for the same type,
// the last registration wins.
func RegisterCaller(t reflect.Type, maker func(interface{}) Caller) {
	callersMu.Lock()
	defer callersMu.Unlock()

	key := t.String()
	if _, exists := callers[key]; exists {
		log.Warnf(context.Background(), "Caller for %v already registered. Overwriting.", key)
	}
	callers[key] = maker
}

// MakeCaller returns a caller for given function.
func MakeCaller(fn interface{}) Caller {
	callersMu.Lock()
	maker, exists := callers[reflect.TypeOf(fn).String()]
	callersMu.Unlock()

	if exists {
		return maker(fn)
	}

	// If no specialized implementation is available, we use the standard
	// reflection-based call.

	return &caller{fn: reflect.ValueOf(fn)}
}

type caller struct {
	fn reflect.Value
}

func (c *caller) Type() reflect.Type {
	return c.fn.Type()
}

func (c *caller) Call(args []interface{}) []interface{} {
	return Interface(c.fn.Call(ValueOf(args)))
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
