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

// multiout is a wordcount variation that uses a multi-outout DoFn
// and writes 2 output files.
package main

// beam-playground:
//   name: MultiOut
//   description: An example that counts words in Shakespeare's works and writes 2 output files,
//     -- big - for small words,
//     -- small - for big words.
//   multifile: false
//   pipeline_options: --small sOutput.txt --big bOutput.txt
//   context_line: 86
//   categories:
//     - IO
//     - Options
//     - Branching
//     - Multiple Outputs
//   complexity: MEDIUM
//   tags:
//     - count
//     - io
//     - strings

import (
	"context"
	"flag"
	"fmt"
	"log"
	"regexp"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	input = flag.String("input", "gs://apache-beam-samples/shakespeare/kinglear.txt", "File(s) to read.")
	small = flag.String("small", "", "Output file for small words (required).")
	big   = flag.String("big", "", "Output file for big words (required).")
)

func init() {
	register.Function3x0(splitFn)
	register.Function2x1(formatFn)
	register.Emitter1[string]()
}

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func splitFn(line string, big, small func(string)) {
	for _, word := range wordRE.FindAllString(line, -1) {
		if len(word) > 5 {
			big(word)
		} else {
			small(word)
		}
	}
}

func formatFn(w string, c int) string {
	return fmt.Sprintf("%s: %v", w, c)
}

func writeCounts(s beam.Scope, col beam.PCollection, filename string) {
	counted := stats.Count(s, col)
	textio.Write(s, filename, beam.ParDo(s, formatFn, counted))
}

func main() {
	flag.Parse()
	beam.Init()

	if *small == "" || *big == "" {
		log.Fatal("No outputs provided")
	}

	p := beam.NewPipeline()
	s := p.Root()

	lines := textio.Read(s, *input)
	bcol, scol := beam.ParDo2(s, splitFn, lines)
	writeCounts(s.Scope("Big"), bcol, *big)
	writeCounts(s.Scope("Small"), scol, *small)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
