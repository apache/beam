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

package task

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"strings"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) (beam.PCollection, beam.PCollection) {
	reversed := reverseString(s, input)
	toUpper := toUpperString(s, input)
	return reversed, toUpper
}

func reverseString(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, reverseFn, input)
}

func toUpperString(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, strings.ToUpper, input)
}

func reverseFn(s string) string {
	runes := []rune(s)

	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}

	return string(runes)
}
