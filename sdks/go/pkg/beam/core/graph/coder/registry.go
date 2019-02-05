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

package coder

import (
	"fmt"
	"reflect"
)

var (
	coderRegistry     = make(map[uintptr]func(reflect.Type) *CustomCoder)
	interfaceOrdering []reflect.Type
)

// RegisterCoder registers a user defined coder for a given type, and will
// be used if there is no beam coder for that type. Must be called prior to beam.Init(),
// preferably in an init() function.
//
// Coders are encoder and decoder pairs, and operate around []bytes.
//
// The coder used for a given type follows this ordering:
//   1. Coders for Known Beam types.
//   2. Coders registered for specific types
//   3. Coders registered for interfaces types
//   4. Default coder (JSON)
//
// Types of kind Interface, are handled specially by the registry, so they may be iterated
// over to check if element types implement them.
//
// Repeated registrations of the same type overrides prior ones.
func RegisterCoder(t reflect.Type, enc, dec interface{}) {
	key := tkey(t)

	if _, err := NewCustomCoder(t.String(), t, enc, dec); err != nil {
		panic(fmt.Sprintf("RegisterCoder failed for type %v: %v", t, err))
	}

	if t.Kind() == reflect.Interface {
		// If it's already in the registry, then it's already in the list
		// and should be removed.
		if _, ok := coderRegistry[key]; ok {
			var index int
			for i, iT := range interfaceOrdering {
				iKey := tkey(iT)
				if iKey == key {
					index = i
					break
				}
			}
			interfaceOrdering = append(interfaceOrdering[:index], interfaceOrdering[index+1:]...)
		}
		// Either way, always append.
		interfaceOrdering = append(interfaceOrdering, t)
	}
	name := t.String() // Use the real type names for coders.
	coderRegistry[key] = func(rt reflect.Type) *CustomCoder {
		// We need to provide the concrete type, so that coders that use
		// the reflect.Type have the proper instance.
		cc, err := NewCustomCoder(name, rt, enc, dec)
		if err != nil {
			// An error on look up shouldn't happen after the validation.
			panic(fmt.Sprintf("Creating %v CustomCoder for type %v failed: %v", name, rt, err))
		}
		return cc
	}
}

// LookupCustomCoder returns the custom coder for the type if any,
// first checking for a specific matching type, and then iterating
// through registered interface coders in reverse registration order.
func LookupCustomCoder(t reflect.Type) *CustomCoder {
	key := tkey(t)
	if maker, ok := coderRegistry[key]; ok {
		return maker(t)
	}
	for i := len(interfaceOrdering) - 1; i >= 0; i-- {
		iT := interfaceOrdering[i]
		if t.Implements(iT) {
			key := tkey(iT)
			return coderRegistry[key](t)
		}
	}
	return nil
}

// tkey returns the uintptr for a given type as the key.
func tkey(t reflect.Type) uintptr {
	return reflect.ValueOf(t).Pointer()
}
