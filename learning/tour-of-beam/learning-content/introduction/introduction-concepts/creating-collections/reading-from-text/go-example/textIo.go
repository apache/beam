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
/*
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
// beam-playground:
//   name: TextIO
//   description: TextIO example.
//   multifile: false
//   context_line: 51
//   categories:
//     - Quickstart
//   complexity: BASIC
//   tags:
//     - hellobeam

package main

import (
	"context"
	"fmt"
	"regexp"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/gcs"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/filter"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/top"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
)

func less(a, b string) bool {
	return len(a) < len(b)
}

func main() {
	ctx := context.Background()
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	input := Read(s, "gs://apache-beam-samples/shakespeare/kinglear.txt")

	lines := getLines(s, input)
	fixedSizeLines := top.Largest(s, lines, 10, less)
	output(s, "Lines: ", fixedSizeLines)

	words := getWords(s, lines)
	fixedSizeWords := top.Largest(s, words, 10, less)
	output(s, "Words: ", fixedSizeWords)

	err := beamx.Run(ctx, p)
	if err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}

// Read reads from filename(s) specified by a glob string and a returns a PCollection<string>.
func Read(s beam.Scope, glob string) beam.PCollection {
	return textio.Read(s, glob)
}

// Read text file content line by line. resulting PCollection contains elements, where each element contains a single line of text from the input file.
func getLines(s beam.Scope, input beam.PCollection) beam.PCollection {
	return filter.Include(s, input, func(element string) bool {
		return element != ""
	})
}

// getWords read text lines and split into PCollection of words.
func getWords(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, func(line string, emit func(string)) {
		for _, word := range wordRE.FindAllString(line, -1) {
			emit(word)
		}
	}, input)
}

func output(s beam.Scope, prefix string, input beam.PCollection) {
	beam.ParDo0(s, func(elements []string) {
		for _, element := range elements {
			fmt.Println(prefix, element)
		}
	}, input)
}
