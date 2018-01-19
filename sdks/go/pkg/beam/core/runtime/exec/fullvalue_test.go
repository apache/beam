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
	"reflect"
)

func makeValues(vs ...interface{}) []FullValue {
	var ret []FullValue
	for _, v := range vs {
		ret = append(ret, FullValue{Elm: v})
	}
	return ret
}

func extractValues(vs ...FullValue) []interface{} {
	var ret []interface{}
	for _, v := range vs {
		ret = append(ret, v.Elm)
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
	if (a.Elm == nil) != (b.Elm == nil) {
		return false
	}
	if (a.Elm2 == nil) != (b.Elm2 == nil) {
		return false
	}

	if a.Elm != nil {
		if !reflect.DeepEqual(a.Elm, b.Elm) {
			return false
		}
	}
	if a.Elm2 != nil {
		if !reflect.DeepEqual(a.Elm2, b.Elm2) {
			return false
		}
	}
	return true
}
