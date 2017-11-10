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

// minimal_wordcount is an example that counts words in Shakespeare.
//
// This example is the first in a series of four successively more detailed
// 'word count' examples. Here, for simplicity, we don't show any
// error-checking or argument processing, and focus on construction of the
// pipeline, which chains together the application of core transforms.
//
// Next, see the wordcount pipeline, then the debugging_wordcount pipeline, and
// finally the windowed_wordcount pipeline, for more detailed examples that
// introduce additional concepts.
//
// Concepts:
//
//   1. Reading data from text files
//   2. Specifying 'inline' transforms
//   3. Counting items in a PCollection
//   4. Writing data to text files
//
// No arguments are required to run this pipeline. It will be executed with
// the direct runner. You can see the results in the output file named
// "wordcounts.txt" in your current working directory.
package main

import (
	"context"
	"fmt"
	"regexp"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	_ "github.com/apache/beam/sdks/go/pkg/beam/io/textio/gcs"
	_ "github.com/apache/beam/sdks/go/pkg/beam/io/textio/local"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/direct"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func main() {
	// Create the Pipeline object and root scope.
	p := beam.NewPipeline()
	s := p.Root()

	// Apply the pipeline's transforms.

	// Concept #1: Invoke a root transform with the pipeline; in this case,
	// textio.Read to read a set of input text file. textio.Read returns a
	// PCollection where each element is one line from the input text
	// (one of of Shakespeare's texts).

	// This example reads a public data set consisting of the complete works
	// of Shakespeare.
	lines := textio.Read(s, "gs://apache-beam-samples/shakespeare/*")

	// Concept #2: Invoke a ParDo transform on our PCollection of text lines.
	// This ParDo invokes a DoFn (defined in-line) on each element that
	// tokenizes the text line into individual words. The ParDo returns a
	// PCollection of type string, where each element is an individual word in
	// Shakespeare's collected texts.
	words := beam.ParDo(s, func(line string, emit func(string)) {
		for _, word := range wordRE.FindAllString(line, -1) {
			emit(word)
		}
	}, lines)

	// Concept #3: Invoke the stats.Count transform on our PCollection of
	// individual words. The Count transform returns a new PCollection of
	// key/value pairs, where each key represents a unique word in the text.
	// The associated value is the occurrence count for that word.
	counted := stats.Count(s, words)

	// Use a ParDo to format our PCollection of word counts into a printable
	// string, suitable for writing to an output file. When each element
	// produces exactly one element, the DoFn can simply return it.
	formatted := beam.ParDo(s, func(w string, c int) string {
		return fmt.Sprintf("%s: %v", w, c)
	}, counted)

	// Concept #4: Invoke textio.Write at the end of the pipeline to write
	// the contents of a PCollection (in this case, our PCollection of
	// formatted strings) to a text file.
	textio.Write(s, "wordcounts.txt", formatted)

	// Run the pipeline on the direct runner.
	direct.Execute(context.Background(), p)
}
