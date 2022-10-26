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
//   name: CommonTransformsSolution
//   description: Common Transforms motivating challenge solution.
//   multifile: false
//   context_line: 39
//   categories:
//     - Quickstart
//   complexity: BASIC
//   tags:
//     - hellobeam

package main

import (
	"context"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/filter"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
    "github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
)

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

    file := Read(s, "gs://apache-beam-samples/shakespeare/kinglear.txt")

    input := applyTransform(s, file)

	filtered := getPositiveNumbers(s,input)

	tagged := getMap(s,filtered)

	count := getCountingNumbersByKey(s, tagged)

	debug.Print(s, count)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

func applyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
    return beam.ParDo(s, func(line string) float64 {
        taxi := strings.Split(strings.TrimSpace(line), ",")
        if len(taxi) > 16 {
            cost, _ := strconv.ParseFloat(taxi[16],64)
            return cost
        }
        return 0.0
    }, input)
}

// Returns positive numbers
func getPositiveNumbers(s beam.Scope, input beam.PCollection) beam.PCollection{
  return filter.Include(s, input, func(element int) bool {
         		return element >= 0
            })
}
// Write here getMap function
func getMap(s beam.Scope, input beam.PCollection) beam.PCollection{
  return beam.ParDo(s, func(in int) (string, int) {
         		if in%2 == 0 {
         			return "even", in
         		} else {
         			return "odd", in
         		}
         	}, input)

}
// Returns the count of numbers
func getCountingNumbersByKey(s beam.Scope, input beam.PCollection) beam.PCollection {
	return stats.Count(s,
		beam.ParDo(s, func(key string, value int) string {
			return key
		}, input))
}
