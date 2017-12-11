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

import "reflect"

func makeValue(v interface{}) FullValue {
	return FullValue{Elm: reflect.ValueOf(v)}
}

func makeValues(vs ...interface{}) []FullValue {
	var ret []FullValue
	for _, v := range vs {
		ret = append(ret, makeValue(v))
	}
	return ret
}

func extractValue(v FullValue) interface{} {
	return v.Elm.Interface()
}

func extractValues(vs ...FullValue) []interface{} {
	var ret []interface{}
	for _, v := range vs {
		ret = append(ret, extractValue(v))
	}
	return ret
}

func equalList(a, b []FullValue) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if !equal(v, b[i]) {
			return false
		}
	}
	return true
}

func equal(a, b FullValue) bool {
	if a.Timestamp != b.Timestamp {
		return false
	}
	if (a.Elm.Kind() == reflect.Invalid) != (b.Elm.Kind() == reflect.Invalid) {
		return false
	}
	if (a.Elm2.Kind() == reflect.Invalid) != (b.Elm2.Kind() == reflect.Invalid) {
		return false
	}

	if a.Elm.Kind() != reflect.Invalid {
		if !reflect.DeepEqual(a.Elm.Interface(), b.Elm.Interface()) {
			return false
		}
	}
	if a.Elm2.Kind() != reflect.Invalid {
		if !reflect.DeepEqual(a.Elm2.Interface(), b.Elm2.Interface()) {
			return false
		}
	}
	return true
}
