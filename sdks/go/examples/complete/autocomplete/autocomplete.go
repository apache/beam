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

import (
	"context"
	"flag"
	"os"
	"regexp"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/top"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

// TODO(herohde) 5/30/2017: fully implement https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/complete/AutoComplete.java

var (
	input = flag.String("input", os.ExpandEnv("$GOPATH/src/github.com/apache/beam/sdks/go/data/haiku/old_pond.txt"), "Files to read.")
	n     = flag.Int("top", 3, "Number of completions")
)

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func init() {
	register.Function2x0(extractFn)
	register.Function2x1(less)
	register.Emitter1[string]()
}

func extractFn(line string, emit func(string)) {
	for _, word := range wordRE.FindAllString(line, -1) {
		emit(word)
	}
}

func less(a, b string) bool {
	return len(a) < len(b)
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	log.Info(ctx, "Running autocomplete")

	p := beam.NewPipeline()
	s := p.Root()
	lines, err := textio.Immediate(s, *input)
	if err != nil {
		log.Exitf(ctx, "Failed to read %v: %v", *input, err)
	}
	words := beam.ParDo(s, extractFn, lines)

	hits := top.Largest(s, words, *n, less)
	debug.Print(s, hits)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
