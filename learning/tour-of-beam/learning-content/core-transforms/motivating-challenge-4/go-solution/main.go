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

//   beam-playground:
//     name: CoreTransformsSolution4
//     description: Core Transforms fourth motivating challenge.
//     multifile: false
//     context_line: 53
//     categories:
//       - Quickstart
//     complexity: BASIC
//     tags:
//       - hellobeam

package main

import (
	"context"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/filter"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/top"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
	"regexp"
	"strings"
)

func less(a, b string) bool {
	return true
}

var (
	result = make(map[string][]string)
)

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

	input := textio.Read(s, "gs://apache-beam-samples/shakespeare/kinglear.txt")

	lines := getLines(s, input)

	fixedSizeLines := top.Largest(s, lines, 100, less)

	words := getWords(s, fixedSizeLines)

	groupedPCollection := groupWordsByFirstLetter(s, words)

	debug.Print(s, groupedPCollection)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

func getLines(s beam.Scope, input beam.PCollection) beam.PCollection {
	return filter.Include(s, input, func(element string) bool {
		return element != ""
	})
}

func getWords(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, func(line []string, emit func(string)) {
		for _, word := range line {
			e := strings.Split(word, " ")
			for _, element := range e {
				reg := regexp.MustCompile(`([^\w])`)
				res := reg.ReplaceAllString(element, "")
				emit(res)
			}
		}
	}, input)
}

type groupWordByFirstLetterFn struct{}

type wordAccum struct {
	Result  map[string][]string
	Current []map[string]string
}

func groupWordsByFirstLetter(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.Combine(s, &groupWordByFirstLetterFn{}, input)
}

func (c *groupWordByFirstLetterFn) CreateAccumulator() wordAccum {
	return wordAccum{}
}

func (c *groupWordByFirstLetterFn) AddInput(accum wordAccum, input string) wordAccum {
	firsLetterAndWord := make(map[string]string)
	if len(input) > 0 {
		firsLetterAndWord[string(input[0])] = input

		accum.Current = append(accum.Current, firsLetterAndWord)

		// MergeAccumulators logic
		for _, element := range accum.Current {
			for k, v := range element {
				value, ok := result[k]
				if ok {
					value = append(value, v)
				}
				result[k] = value
			}
		}
	}
	return accum
}

func (c *groupWordByFirstLetterFn) MergeAccumulators(accumA, accumB wordAccum) wordAccum {
	// Use logic for each accum
	return accumA
}

func (c *groupWordByFirstLetterFn) ExtractOutput(accum wordAccum) map[string][]string {
	return result
}
