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

// See: https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/CombinePerKeyExamples.java

import (
	"context"
	"flag"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/bigqueryio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

var (
	input  = flag.String("input", "publicdata:samples.shakespeare", "Shakespeare plays BQ table.")
	output = flag.String("output", "", "Output BQ table.")

	minLength = flag.Int("min_length", 9, "Minimum word length")
)

type WordRow struct {
	Corpus string `bigquery:"corpus"`
	Word   string `bigquery:"word"`
}

type PlaysRow struct {
	Word  string `bigquery:"word"`
	Plays string `bigquery:"plays"`
}

// PlaysForWords generates a string containing the list of play names
// in which that word appears. It takes a PCollection<WordRow> and
// returns a PCollection<PlaysRow>.
func PlaysForWords(s beam.Scope, rows beam.PCollection) beam.PCollection {
	s = s.Scope("PlaysForWords")

	words := beam.ParDo(s, &extractFn{MinLength: *minLength}, rows)
	keyed := beam.CombinePerKey(s, concatFn, words)
	return beam.ParDo(s, formatFn, keyed)
}

// extractFn outputs (word, play) iff the word is longer than the minimum length.
type extractFn struct {
	MinLength int `json:"min_length"`
}

func (f *extractFn) ProcessElement(row WordRow, emit func(string, string)) {
	if len(row.Word) >= f.MinLength {
		emit(row.Word, row.Corpus)
	}
	// TODO(herohde) 7/14/2017: increment counter for "small words"
}

// TODO(herohde) 7/14/2017: the choice of a string (instead of []string) for the
// output makes the combiner simpler. Seems hokey.

func concatFn(a, b string) string {
	return fmt.Sprintf("%v,%v", a, b)
}

func formatFn(word, plays string) PlaysRow {
	return PlaysRow{Word: word, Plays: plays}
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	if *output == "" {
		log.Exit(ctx, "No output table specified. Use --output=<table>")
	}
	project := gcpopts.GetProject(ctx)

	log.Info(ctx, "Running combine")

	p := beam.NewPipeline()
	s := p.Root()
	rows := bigqueryio.Read(s, project, *input, reflect.TypeOf(WordRow{}))
	out := PlaysForWords(s, rows)
	bigqueryio.Write(s, project, *output, out)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
