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
	"strings"
)

// HasTaggedField returns true iff the given struct has a field with any of the
// given tag values.
func HasTaggedField(t reflect.Type, values ...string) bool {
	_, ok := FindTaggedField(t, values...)
	return ok
}

// FindTaggedField returns the field tagged with any of the given tag values, if
// any. The tags are all under the "beam" StructTag key.
func FindTaggedField(t reflect.Type, values ...string) (reflect.StructField, bool) {
	if t == nil || t.Kind() != reflect.Struct {
		return reflect.StructField{}, false
	}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if HasTag(f, values...) {
			return f, true
		}
	}
	return reflect.StructField{}, false
}

// HasTag returns true iff the given field contains one of the given tags
// under the "beam" key.
func HasTag(f reflect.StructField, values ...string) bool {
	list := strings.Split(f.Tag.Get("beam"), ",")
	for _, elm := range list {
		for _, value := range values {
			if elm == value {
				return true
			}
		}
	}
	return false
}

// SetTaggedFieldValue sets s.f = value, where f has the tag "beam:tag". Panics
// if not valid.
func SetTaggedFieldValue(v reflect.Value, tag string, value reflect.Value) {
	f, ok := FindTaggedField(v.Type(), tag)
	if !ok {
		panic(fmt.Sprintf("%v has no field with tag %v", v.Type(), tag))
	}
	SetFieldValue(v, f, value)
}

// SetFieldValue sets s.f = value. Panics if not valid.
func SetFieldValue(s reflect.Value, f reflect.StructField, value reflect.Value) {
	s.FieldByIndex(f.Index).Set(value)
}
