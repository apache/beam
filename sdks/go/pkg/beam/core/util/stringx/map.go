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

package stringx

// Keys returns the domain of a map[string]string.
func Keys(m map[string]string) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Values returns the values of a map[string]string.
func Values(m map[string]string) []string {
	var values []string
	for _, v := range m {
		values = append(values, v)
	}
	return values
}

// AnyValue returns a value of a map[string]string. Panics
// if the map is empty.
func AnyValue(m map[string]string) string {
	for _, v := range m {
		return v
	}
	panic("map empty")
}

// SingleValue returns the single value of a map[string]string.
// Panics if the map does not contain exactly one value.
func SingleValue(m map[string]string) string {
	if len(m) != 1 {
		panic("map not singleton")
	}
	return AnyValue(m)
}
