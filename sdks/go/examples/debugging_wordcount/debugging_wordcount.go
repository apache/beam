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

// debugging_wordcount is an example that verifies word counts in Shakespeare
// and includes Beam best practices.
//
// This example, debugging_wordcount, is the third in a series of four
// successively more detailed 'word count' examples. You may first want to
// take a look at minimal_wordcount and wordcount. After you've looked at
// this example, then see the windowed_wordcount pipeline, for introduction
// of additional concepts.
//
// Basic concepts, also in the minimal_wordcount and wordcount examples:
// Reading text files; counting a PCollection; executing a Pipeline both locally
// and using a selected runner; defining DoFns.
//
// New Concepts:
//
//  1. Using the richer struct DoFn form and accessing optional arguments.
//  2. Logging using the Beam log package, even in a distributed environment
//  3. Testing your Pipeline via passert
//
// To change the runner, specify:
//
//	--runner=YOUR_SELECTED_RUNNER
//
// The input file defaults to a public data set containing the text of King
// Lear, by William Shakespeare. You can override it and choose your own input
// with --input.
package main

// beam-playground:
//   name: DebuggingWordCount
//   description: An example that counts words in Shakespeare's works includes regex filter("Flourish|stomach").
//   multifile: false
//   pipeline_options: --output output.txt
//   context_line: 158
//   categories:
//     - Options
//     - Filtering
//     - Debugging
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - count
//     - io
//     - strings

import (
	"context"
	"flag"
	"fmt"
	"regexp"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

// TODO(herohde) 10/16/2017: support metrics and log level cutoff.

var (
	input  = flag.String("input", "gs://apache-beam-samples/shakespeare/kinglear.txt", "File(s) to read.")
	filter = flag.String("filter", "Flourish|stomach", "Regex filter pattern to use. Only words matching this pattern will be included.")
	output = flag.String("output", "", "Output file (required).")
)

// Concept #1: a DoFn can also be a struct with methods for setup/teardown and
// element/bundle processing. It also allows configuration values to be made
// available at runtime.

func init() {
	// register.DoFnXxY registers a struct DoFn so that it can be correctly serialized and does some optimization
	// to avoid runtime reflection. Since addTimestampFn has 4 inputs and 0 outputs, we use register.DoFn4x0 and provide
	// its input/output types as its constraints.
	// Struct DoFns must be registered for a pipeline to run.
	register.DoFn4x0[context.Context, string, int, func(string, int)](&filterFn{})
	// For simple functional (non-struct) DoFns we can use register.FunctionXxY to perform the same registration without
	// providing type constraints.
	register.Function2x0(extractFn)
	register.Function2x1(formatFn)
	// register.EmitterX is optional and will provide some optimization to make things run faster. Any emitters
	// (functions that produce output for the next step) should be registered. Here we register all emitters with
	// the signature func(string, int).
	register.Emitter2[string, int]()
}

// filterFn is a DoFn for filtering out certain words.
type filterFn struct {
	// Filter is a regex that is serialized as json and available at runtime.
	// Such fields must be exported.
	Filter string `json:"filter"`

	re *regexp.Regexp
}

func (f *filterFn) Setup() {
	f.re = regexp.MustCompile(f.Filter)
}

// Concept #2: The Beam log package should used for all logging in runtime
// functions. The needed context is made available as an argument.

func (f *filterFn) ProcessElement(ctx context.Context, word string, count int, emit func(string, int)) {
	if f.re.MatchString(word) {
		// Log at the "INFO" level each element that we match.
		log.Infof(ctx, "Matched: %v", word)
		emit(word, count)
	} else {
		// Log at the "DEBUG" level each element that is not matched.
		log.Debugf(ctx, "Did not match: %v", word)
	}
}

// The below transforms are identical to the wordcount versions. If this was
// production code, common transforms would be placed in a separate package
// and shared directly rather than being copied.

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

// extractFn is a DoFn that emits the words in a given line.
func extractFn(line string, emit func(string)) {
	for _, word := range wordRE.FindAllString(line, -1) {
		emit(word)
	}
}

// formatFn is a DoFn that formats a word and its count as a string.
func formatFn(w string, c int) string {
	return fmt.Sprintf("%s: %v", w, c)
}

// CountWords is a composite transform that counts the words of an PCollection
// of lines. It expects a PCollection of type string and returns a PCollection
// of type KV<string,int>.
func CountWords(s beam.Scope, lines beam.PCollection) beam.PCollection {
	s = s.Scope("CountWords")
	col := beam.ParDo(s, extractFn, lines)
	return stats.Count(s, col)
}

func main() {
	flag.Parse()
	beam.Init()

	// Concept #2: the beam logging package works both during pipeline
	// construction and at runtime. It should always be used.
	ctx := context.Background()
	if *output == "" {
		log.Exit(ctx, "No output provided")
	}
	if _, err := regexp.Compile(*filter); err != nil {
		log.Exitf(ctx, "Invalid filter: %v", err)
	}

	p := beam.NewPipeline()
	s := p.Root()

	lines := textio.Read(s, *input)
	counted := CountWords(s, lines)
	filtered := beam.ParDo(s, &filterFn{Filter: *filter}, counted)
	formatted := beam.ParDo(s, formatFn, filtered)

	// Concept #3: passert is a set of convenient PTransforms that can be used
	// when writing Pipeline level tests to validate the contents of
	// PCollections. passert is best used in unit tests with small data sets
	// but is demonstrated here as a teaching tool.

	passert.Equals(s, formatted, "Flourish: 3", "stomach: 1")

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
