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

package batch

import (
	"reflect"
)

// defaultElementByteSize reports the byte cost of v for a fixed set of
// primitive types: it is the fallback used when the caller does not
// supply Params.ElementByteSize but BatchSizeBytes > 0.
//
// Returns (size, true) for supported types and (0, false) otherwise.
// For opaque types (user structs, interfaces, maps, non-byte slices,
// channels, functions) callers must supply their own sizer.
func defaultElementByteSize(v any) (int64, bool) {
	switch x := v.(type) {
	case []byte:
		return int64(len(x)), true
	case string:
		return int64(len(x)), true
	case bool:
		return 1, true
	case int8:
		return 1, true
	case uint8:
		return 1, true
	case int16:
		return 2, true
	case uint16:
		return 2, true
	case int32:
		return 4, true
	case uint32:
		return 4, true
	case float32:
		return 4, true
	case int:
		return 8, true
	case uint:
		return 8, true
	case int64:
		return 8, true
	case uint64:
		return 8, true
	case float64:
		return 8, true
	}
	return 0, false
}

// isBuiltinSizeable reports whether defaultElementByteSize can size an
// element of type t. Used at pipeline-build time to fail fast when
// BatchSizeBytes > 0 is requested without a user-supplied
// ElementByteSize and the value type is not one of the supported
// primitives.
//
// A []byte is recognized via reflect.Slice with Uint8 element kind; any
// other slice is not sizeable by the built-in fallback.
func isBuiltinSizeable(t reflect.Type) bool {
	if t == nil {
		return false
	}
	switch t.Kind() {
	case reflect.String,
		reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	case reflect.Slice:
		return t.Elem().Kind() == reflect.Uint8
	}
	return false
}
