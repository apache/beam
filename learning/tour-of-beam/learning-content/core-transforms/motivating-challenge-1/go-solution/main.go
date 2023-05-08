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
//   name: CoreTransformsSolution1
//   description: Core Transforms first motivating challenge.
//   multifile: false
//   context_line: 54
//   categories:
//     - Quickstart
//   complexity: BASIC
//   tags:
//     - hellobeam

package main

import (
	"context"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/filter"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/top"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
	"regexp"
	"strings"
)

var (
	wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
)

func less(a, b string) bool {
	return true
}

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

	input := textio.Read(s, "gs://apache-beam-samples/shakespeare/kinglear.txt")

	lines := getLines(s, input)

	fixedSizeLines := top.Largest(s, lines, 100, less)

	words := getWords(s, fixedSizeLines)

	allCaseWords := partitionPCollectionByCase(s, words)

	upperCaseWords := countPerElement(s, allCaseWords[0])
	capitalCaseWords := countPerElement(s, allCaseWords[1])
	lowerCaseWords := countPerElement(s, allCaseWords[2])

	newFirstPartPCollection := convertPCollectionToLowerCase(s, upperCaseWords)
	newSecondPartPCollection := convertPCollectionToLowerCase(s, capitalCaseWords)

	flattenPCollection := mergePCollections(s, newFirstPartPCollection, newSecondPartPCollection, lowerCaseWords)

	groupByKeyPCollection := groupByKey(s, flattenPCollection)

	debug.Print(s, groupByKeyPCollection)

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

func partitionPCollectionByCase(s beam.Scope, input beam.PCollection) []beam.PCollection {
	return beam.Partition(s, 3, func(word string) int {
		if word == strings.ToUpper(word) {
			return 0
		}
		if word == strings.Title(strings.ToLower(word)) {
			return 1
		}
		return 2

	}, input)
}

func countPerElement(s beam.Scope, input beam.PCollection) beam.PCollection {
	return stats.Count(s, input)
}

func convertPCollectionToLowerCase(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, func(word string, value int) (string, int) {
		return strings.ToLower(word), value
	}, input)
}

func mergePCollections(s beam.Scope, aInput beam.PCollection, bInput beam.PCollection, cInput beam.PCollection) beam.PCollection {
	return beam.Flatten(s, aInput, bInput, cInput)
}

func groupByKey(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.GroupByKey(s, input)
}
