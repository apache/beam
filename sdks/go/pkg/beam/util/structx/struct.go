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

// Package structx provides utilities for working with structs.
package structx

import (
	"fmt"
	"reflect"
	"strings"
)

// InferFieldNames returns the field names of the given struct type and tag key. Includes only
// exported fields. If a field's tag key is empty or not set, uses the field's name. If a field's
// tag key is set to '-', omits the field. Panics if the type's kind is not a struct.
func InferFieldNames(t reflect.Type, key string) []string {
	if t.Kind() != reflect.Struct {
		panic(fmt.Sprintf("structx: InferFieldNames of non-struct type %s", t))
	}

	var names []string

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		if field.Anonymous {
			names = append(names, InferFieldNames(field.Type, key)...)
			continue
		}

		if !field.IsExported() {
			continue
		}

		value := field.Tag.Get(key)
		name := strings.Split(value, ",")[0]

		if name == "" {
			names = append(names, field.Name)
		} else if name != "-" {
			names = append(names, name)
		}
	}

	return names
}

// FieldIndexByTag returns the index of the field with the given tag key and value. Returns -1 if
// the field is not found. Panics if the type's kind is not a struct.
func FieldIndexByTag(t reflect.Type, key string, value string) int {
	if t.Kind() != reflect.Struct {
		panic(fmt.Sprintf("structx: FieldIndexByTag of non-struct type %s", t))
	}

	for i := 0; i < t.NumField(); i++ {
		values := t.Field(i).Tag.Get(key)
		name := strings.Split(values, ",")[0]

		if name == value {
			return i
		}
	}

	return -1
}
