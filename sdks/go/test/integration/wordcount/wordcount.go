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

// Package wordcount contains transforms for wordcount.
package wordcount

import (
	"context"
	"regexp"
	"strings"

	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

var (
	wordRE  = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
	empty   = beam.NewCounter("extract", "emptyLines")
	lineLen = beam.NewDistribution("extract", "lineLenDistro")
)

// CountWords is a composite transform that counts the words of a PCollection
// of lines. It expects a PCollection of type string and returns a PCollection
// of type KV<string,int>. The Beam type checker enforces these constraints
// during pipeline construction.
func CountWords(s beam.Scope, lines beam.PCollection) beam.PCollection {
	s = s.Scope("CountWords")

	// Convert lines of text into individual words.
	col := beam.ParDo(s, extractFn, lines)

	// Count the number of times each word occurs.
	return stats.Count(s, col)
}

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

// Format formats a KV of a word and its count as a string.
func Format(s beam.Scope, counted beam.PCollection) beam.PCollection {
	return beam.ParDo(s, formatFn, counted)
}

func formatFn(w string, c int) string {
	return fmt.Sprintf("%s: %v", w, c)
}

// WordCount returns a self-validating wordcount pipeline.
func WordCount(glob, hash string, size int) *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	in := textio.Read(s, glob)
	out := Format(s, CountWords(s, in))
	passert.Hash(s, out, "out", hash, size)

	return p
}
