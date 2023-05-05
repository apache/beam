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
//   name: additional-outputs
//   description: Additional outputs example.
//   multifile: false
//   context_line: 37
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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

	// List of elements
	input := beam.Create(s, 10, 50, 120, 20, 200, 0)

	// The applyTransform() converts [input] to [numBelow100] and [numAbove100]
	numBelow100, numAbove100 := applyTransform(s, input)

	debug.Printf(s, "Number <= 100: %v", numBelow100)
	debug.Printf(s, "Number > 100: %v", numAbove100)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

// The function has multiple outputs, numbers above 100 and below
func applyTransform(s beam.Scope, input beam.PCollection) (beam.PCollection, beam.PCollection) {
	return beam.ParDo2(s, func(element int, numBelow100, numAbove100 func(int)) {
		if element <= 100 {
			numBelow100(element)
			return
		}
		numAbove100(element)
	}, input)
}
