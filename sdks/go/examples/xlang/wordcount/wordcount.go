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

// wordcount exemplifies using a cross-language Count transform from a test
// expansion service to count words.
//
// Prerequisites to run wordcount:
// –> [Required] Job needs to be submitted to a portable runner (--runner=universal)
// –> [Required] Endpoint of job service needs to be passed (--endpoint=<ip:port>)
// –> [Required] Endpoint of expansion service needs to be passed (--expansion_addr=<ip:port>)
// –> [Optional] Environment type can be LOOPBACK. Defaults to DOCKER. (--environment_type=LOOPBACK|DOCKER)
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"

	// Imports to enable correct filesystem access and runner setup in LOOPBACK mode
	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/gcs"
	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/local"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/universal"
)

var (
	expansionAddr = flag.String("expansion_addr", "", "Address of Expansion Service")
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
	return fmt.Sprintf("%s:%v", w, c)
}

func init() {
	beam.RegisterFunction(extractFn)
	beam.RegisterFunction(formatFn)
}

func main() {
	flag.Parse()
	beam.Init()

	if *expansionAddr == "" {
		log.Fatal("No expansion address provided")
	}

	p := beam.NewPipeline()
	s := p.Root()

	lines := beam.CreateList(s, strings.Split(lorem, "\n"))
	col := beam.ParDo(s, extractFn, lines)

	// Using the cross-language transform
	outputType := typex.NewKV(typex.New(reflectx.String), typex.New(reflectx.Int64))
	counted := beam.CrossLanguageWithSingleInputOutput(s, "beam:transforms:xlang:count", nil, *expansionAddr, col, outputType)

	formatted := beam.ParDo(s, formatFn, counted)
	passert.Equals(s, formatted, "a:4", "b:4", "c:5")

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

var lorem = `a b b c
b c a
a b c
c
a
c
`
