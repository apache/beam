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
//   name: CoreTransformsChallenge3
//   description: Core Transforms third motivating challenge.
//   multifile: false
//   context_line: 44
//   categories:
//     - Quickstart
//   complexity: BASIC
//   tags:
//     - hellobeam

package main

import (
	"context"
    _ "strings"
	"regexp"
    "github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/filter"
    "github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
    _ "github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
)

var (
    wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
)

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

    file := textio.Read(s, "gs://apache-beam-samples/shakespeare/kinglear.txt")

    lines := getLines(s, file)

    words := getWords(s,lines)

    distinctWordsStartLetterS := getCompositeWordsStartWith(s,words)

    wordWithUpperCase, wordWithLowerCase := getMultiplePCollections(s,distinctWordsStartLetterS)

    result := checkExistUpperWordsInLowerCaseView(s,wordWithUpperCase,wordWithLowerCase)


    debug.Print(s,result)


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
    return beam.ParDo(s, func(line string, emit func(string)) {
        for _, word := range wordRE.FindAllString(line, -1) {
            emit(word)
        }
    }, input)
}

func getCompositeWordsStartWith(s beam.Scope, input beam.PCollection) beam.PCollection{
   return input
}

func getMultiplePCollections(s beam.Scope, input beam.PCollection) (beam.PCollection, beam.PCollection){
    return input,input
}

func checkExistUpperWordsInLowerCaseView(s beam.Scope,wordWithUpperCase beam.PCollection,wordWithLowerCase beam.PCollection) beam.PCollection{
    return wordWithLowerCase
}

func compareFn(wordWithUpperCase string, wordWithLowerCase func(*string) bool, emit func(string)) {
	emit(wordWithUpperCase)
}