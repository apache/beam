/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// beam-playground:
//   name: CSV
//   description: CSV example.
//   multifile: false
//   context_line: 49
//   categories:
//     - Quickstart
//   complexity: BASIC
//   tags:
//     - hellobeam

package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/top"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

func less(a, b float64) bool {
	return a < b
}

func main() {
	ctx := context.Background()
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	input := Read(s, "gs://apache-beam-samples/nyc_taxi/misc/sample1000.csv")

	cost := applyTransform(s, input)

	fixedSizeElements := top.Largest(s, cost, 10, less)

	output(s, "Total cost: ", fixedSizeElements)

	err := beamx.Run(ctx, p)
	if err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}

// Read reads from filename(s) specified by a glob string and a returns a PCollection<string>.
func Read(s beam.Scope, glob string) beam.PCollection {
	return textio.Read(s, glob)
}

// ApplyTransform converts to float total_amount from all the elements in a PCollection<string>.
func applyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, func(line string) float64 {
		taxi := strings.Split(strings.TrimSpace(line), ",")
		if len(taxi) > 16 {
			cost, _ := strconv.ParseFloat(taxi[16], 64)
			return cost
		}
		return 0.0
	}, input)
}

func output(s beam.Scope, prefix string, input beam.PCollection) {
	beam.ParDo0(s, func(elements []float64) {
		for _, element := range elements {
			fmt.Println(prefix, element)
		}
	}, input)
}
