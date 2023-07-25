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

package main

// beam-playground:
//   name: Contains
//   description: An example counts received substring in Shakespeare's works.
//   multifile: false
//   pipeline_options: --search king
//   context_line: 95
//   categories:
//     - Filtering
//     - Options
//     - Debugging
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
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

// Options used purely at pipeline construction-time can just be flags.
var (
	input  = flag.String("input", "gs://apache-beam-samples/shakespeare/kinglear.txt", "File(s) to read.")
	search = flag.String("search", "", "Only return words that contain this substring.")
)

func init() {
	register.Function2x0(extractFn)
	register.Function2x1(formatFn)
	register.DoFn2x0[string, func(string)](&includeFn{})
	register.Emitter1[string]()
}

// FilterWords returns PCollection<KV<word,count>> with (up to) 10 matching words.
func FilterWords(s beam.Scope, lines beam.PCollection) beam.PCollection {
	s = s.Scope("FilterWords")
	words := beam.ParDo(s, extractFn, lines)
	filtered := beam.ParDo(s, &includeFn{Search: *search}, words)
	counted := stats.Count(s, filtered)
	return debug.Head(s, counted, 10)
}

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func extractFn(line string, emit func(string)) {
	for _, w := range wordRE.FindAllString(line, -1) {
		emit(w)
	}
}

// includeFn outputs (word) iif the word contains substring Search.
type includeFn struct {
	Search string `json:"search"`
}

func (f *includeFn) ProcessElement(s string, emit func(string)) {
	if strings.Contains(s, f.Search) {
		emit(s)
	}
}

func formatFn(w string, c int) string {
	return fmt.Sprintf("%s: %v", w, c)
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	if *search == "" {
		log.Exit(ctx, "No search string provided. Use --search=foo")
	}

	log.Info(ctx, "Running contains")

	// Construct a pipeline that only keeps 10 words that contain the provided search string.
	p := beam.NewPipeline()
	s := p.Root()
	lines := textio.Read(s, *input)
	filtered := FilterWords(s, lines)
	formatted := beam.ParDo(s, formatFn, filtered)
	debug.Print(s, formatted)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
