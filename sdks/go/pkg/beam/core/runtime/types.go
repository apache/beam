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
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

var types = make(map[string]reflect.Type)

// RegisterType inserts "external" types into a global type registry to bypass
// serialization and preserve full method information. It should be called in
// init() only. Returns the external key for the type.
func RegisterType(t reflect.Type) string {
	if initialized {
		panic("Init hooks have already run. Register type during init() instead.")
	}

	t = reflectx.SkipPtr(t)

	k, ok := TypeKey(t)
	if !ok {
		panic(fmt.Sprintf("invalid registration type: %v", t))
	}

	if v, exists := types[k]; exists && v != t {
		panic(fmt.Sprintf("type already registered for %v, and new type %v != %v (existing type)", k, t, v))
	}
	types[k] = t
	return k
}

// LookupType looks up a type in the global type registry by external key.
func LookupType(key string) (reflect.Type, bool) {
	t, ok := types[key]
	return t, ok
}

// TypeKey returns the external key of a given type. Returns false if not a
// candidate for registration.
func TypeKey(t reflect.Type) (string, bool) {
	if t.PkgPath() == "" || t.Name() == "" {
		return "", false // no pre-declared or unnamed types
	}
	return fmt.Sprintf("%v.%v", t.PkgPath(), t.Name()), true
}
