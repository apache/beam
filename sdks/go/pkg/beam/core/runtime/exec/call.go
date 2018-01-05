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

package exec

import (
	"context"
	"reflect"
	"sync"

	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

//go:generate specialize --input=callers.tmpl --x=data,universals

// Caller is a uniform function call interface for a function. This indirection allows
// us to avoid reflection call overhead for certain types.
type Caller interface {
	// Call invokes the implicit fn with arguments.
	Call(args []reflect.Value) []reflect.Value
}

// TODO(herohde) 1/4/2018: Consider forms that avoid the slice creation as well,
// i.e.,
//         Call2x1(a, b reflect.Value) reflect.Value or
//         Call2bool(a, b interface{}) bool
// signatures. If we do that, we should probably move this mechanism into a
// separate "fastcall" package.

var (
	callers   = make(map[string]func(reflect.Value) Caller)
	callersMu sync.Mutex
)

// RegisterCaller registers an custom caller for the given type, such as
// "func(int)bool". If multiple callers are registered for the same type,
// the last registration wins.
func RegisterCaller(t reflect.Type, maker func(reflect.Value) Caller) {
	callersMu.Lock()
	defer callersMu.Unlock()

	key := t.String()
	if _, exists := callers[key]; exists {
		log.Warnf(context.Background(), "Caller for %v already registered. Overwriting.", key)
	}
	callers[key] = maker
}

func makeCaller(fn reflect.Value) Caller {
	encodersMu.Lock()
	maker, exists := callers[fn.Type().String()]
	encodersMu.Unlock()

	if exists {
		return maker(fn)
	}

	// If no specialized implementation is available, we use the standard
	// reflection-based call.

	return &caller{fn: fn}
}

type caller struct {
	fn reflect.Value
}

func (c *caller) Call(args []reflect.Value) []reflect.Value {
	return c.fn.Call(args)
}
