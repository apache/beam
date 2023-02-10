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
	"fmt"
	"reflect"
)

// ShallowClone creates a shallow copy of the given value. Most
// useful for slices and maps.
func ShallowClone(v any) any {
	if v == nil {
		return nil
	}
	val := reflect.ValueOf(v)

	t := val.Type()
	switch t.Kind() {
	case reflect.Slice:
		if val.IsNil() {
			return reflect.Zero(t).Interface() // don't allocate for zero values
		}

		size := val.Len()
		ret := reflect.MakeSlice(t, size, size)
		for i := 0; i < size; i++ {
			ret.Index(i).Set(val.Index(i))
		}
		return ret.Interface()

	case reflect.Map:
		if val.IsNil() {
			return reflect.Zero(t).Interface() // don't allocate for zero values
		}

		ret := reflect.MakeMapWithSize(t, val.Len())
		keys := val.MapKeys()
		for _, key := range keys {
			ret.SetMapIndex(key, val.MapIndex(key))
		}
		return ret.Interface()

	case reflect.Array, reflect.Chan, reflect.Interface, reflect.Func, reflect.Invalid:
		panic(fmt.Sprintf("unsupported type for clone: %v", t))

	default:
		return v
	}
}

// UpdateMap merges two maps of type map[K]*V, with the second overwriting values
// into the first (and mutating it). If the overwriting value is nil, the key is
// deleted.
func UpdateMap(base, updates any) {
	if updates == nil {
		return // ok: nop
	}
	if base == nil {
		panic("base map cannot be nil")
	}

	m := reflect.ValueOf(base)
	o := reflect.ValueOf(updates)

	if o.Type().Kind() != reflect.Map || m.Type() != o.Type() {
		panic(fmt.Sprintf("invalid types for map update: %v != %v", m.Type(), o.Type()))
	}

	keys := o.MapKeys()
	for _, key := range keys {
		val := o.MapIndex(key)
		if val.IsNil() {
			m.SetMapIndex(key, reflect.Value{}) // delete
		} else {
			m.SetMapIndex(key, val)
		}
	}
}
