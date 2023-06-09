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
//     name: CoreTransformsSolution3
//     description: Core Transforms third motivating challenge.
//     multifile: false
//     context_line: 51
//     categories:
//       - Quickstart
//     complexity: BASIC
//     tags:
//       - hellobeam

package main

import (
	"context"
	"fmt"
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

	distinctWordsStartLetterI := getCompositeWordsStartWith(s, words)

	wordWithUpperCase, wordWithLowerCase := getMultiplePCollections(s, distinctWordsStartLetterI)

	result := checkExistUpperWordsInLowerCaseView(s, wordWithUpperCase, wordWithLowerCase)

	debug.Print(s, result)

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

func getCompositeWordsStartWith(s beam.Scope, input beam.PCollection) beam.PCollection {
	wordsStartWithLetterS := filter.Include(s, input, func(element string) bool {
		return strings.HasPrefix(strings.ToLower(element), "i")
	})

	wordWithCount := stats.Count(s, wordsStartWithLetterS)

	return beam.ParDo(s, func(word string, count int) string {
		return word
	}, wordWithCount)
}

func getMultiplePCollections(s beam.Scope, input beam.PCollection) (beam.PCollection, beam.PCollection) {
	return beam.ParDo2(s, func(element string, wordWithUpperCase, wordWithLowerCase func(string)) {
		if strings.HasPrefix(element, "I") {
			wordWithUpperCase(element)
			return
		}
		wordWithLowerCase(element)
	}, input)
}

func checkExistUpperWordsInLowerCaseView(s beam.Scope, wordWithUpperCase beam.PCollection, wordWithLowerCase beam.PCollection) beam.PCollection {
	option := beam.SideInput{
		Input: wordWithLowerCase,
	}
	return beam.ParDo(s, compareFn, wordWithUpperCase, option)
}

func compareFn(wordWithUpperCase string, wordWithLowerCase func(*string) bool, emit func(string)) {
	var word string
	for wordWithLowerCase(&word) {
		if strings.ToLower(wordWithUpperCase) == word {
			fmt.Print(word)
			emit(word)
		}
	}
}
