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

// beam-playground:
//   name: composite
//   description: Composite example.
//   multifile: false
//   context_line: 38
//   categories:
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - hellobeam

package main

import (
	"context"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

	// Getting a list of items
	input := createLines(s)

	// The applyTransform() converts [input] to [output]
	output := applyTransform(s, input)

	debug.Print(s, output)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

// Divides sentences into words and returns their numbers
func applyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	s = s.Scope("CountCharacters")
	characters := extractNonSpaceCharacters(s, input)
	return stats.Count(s, characters)
}

// Nested logic that collects characters
func extractNonSpaceCharacters(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, func(line string, emit func(string)) {
		for _, k := range line {
			char := string(k)
			if char != " " {
				emit(char)
			}
		}
	}, input)
}

// Function for create list of elements
func createLines(s beam.Scope) beam.PCollection {
	return beam.Create(s,
		"Apache Beam is an open source unified programming model",
		"to define and execute data processing pipelines")
}
