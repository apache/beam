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

//   beam-playground:
//     name: branching
//     description: Branching example.
//     multifile: false
//     context_line: 41
//     categories:
//       - Quickstart
//     complexity: MEDIUM
//     tags:
//       - hellobeam

package main

import (
	"context"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
	"strings"
)

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

	// List of elements
	input := beam.Create(s, "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog")

	// The applyTransform() converts [input] to [reversed] and [toUpper]
	reversed, toUpper := applyTransform(s, input)

	debug.Printf(s, "Reversed: %s", reversed)

	debug.Printf(s, "Upper: %s", toUpper)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

// The applyTransform accept PCollection and return new 2 PCollection
func applyTransform(s beam.Scope, input beam.PCollection) (beam.PCollection, beam.PCollection) {
	reversed := reverseString(s, input)
	toUpper := toUpperString(s, input)
	return reversed, toUpper
}

// This function return PCollection with reversed elements
func reverseString(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, reverseFn, input)
}

// This function return PCollection return elements with Upper case
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
