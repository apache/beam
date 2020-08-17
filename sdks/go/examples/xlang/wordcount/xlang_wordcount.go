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

// xlang_wordcount exemplifies using a cross language transform from Python to count words
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"

	// Imports to enable correct filesystem access and runner setup in LOOPBACK mode
	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/gcs"
	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/local"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/universal"
)

var (
	// Set this option to choose a different input file or glob.
	input = flag.String("input", "./input", "File(s) to read.")

	// Set this required option to specify where to write the output.
	output = flag.String("output", "./output", "Output file (required).")

	expansionAddr = flag.String("expansion-addr", "", "Address of Expansion Service")
)

var (
	wordRE  = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
	empty   = beam.NewCounter("extract", "emptyLines")
	lineLen = beam.NewDistribution("extract", "lineLenDistro")
)

// extractFn is a DoFn that emits the words in a given line.
func extractFn(ctx context.Context, line string, emit func(string)) {
	lineLen.Update(ctx, int64(len(line)))
	if len(strings.TrimSpace(line)) == 0 {
		empty.Inc(ctx, 1)
	}
	for _, word := range wordRE.FindAllString(line, -1) {
		emit(word)
	}
}

// formatFn is a DoFn that formats a word and its count as a string.
func formatFn(w string, c int64) string {
	return fmt.Sprintf("%s: %v", w, c)
}

func init() {
	beam.RegisterFunction(extractFn)
	beam.RegisterFunction(formatFn)
}

func main() {
	flag.Parse()
	beam.Init()

	if *output == "" {
		log.Fatal("No output provided")
	}

	if *expansionAddr == "" {
		log.Fatal("No expansion address provided")
	}

	p := beam.NewPipeline()
	s := p.Root()

	lines := textio.Read(s, *input)
	col := beam.ParDo(s, extractFn, lines)

	// Using Cross-language Count from Python's test expansion service
	outputType := typex.NewKV(typex.New(reflectx.String), typex.New(reflectx.Int64))
	counted := beam.CrossLanguage(s,
		"beam:transforms:xlang:count",
		nil,
		*expansionAddr,
		map[string]beam.PCollection{"xlang-in": col},
		map[string]typex.FullType{"None": outputType},
	)

	formatted := beam.ParDo(s, formatFn, counted["None"])
	textio.Write(s, *output, formatted)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
