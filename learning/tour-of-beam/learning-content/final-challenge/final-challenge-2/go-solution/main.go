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
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// beam-playground:
//   name: FinalSolution2
//   description: Final challenge solution 2.
//   multifile: false
//   context_line: 54
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

package main

import (
	"context"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window/trigger"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
	"os"
	"regexp"
	"strings"
	"time"
)

type Analysis struct {
	Word         string
	Negative     string
	Positive     string
	Uncertainty  string
	Litigious    string
	Strong       string
	Weak         string
	Constraining string
}

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func main() {
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "") // FILL IN WITH YOUR FILE PATH

	ctx := context.Background()

	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	shakespeare := textio.Read(s, "gs://apache-beam-samples/shakespeare/kinglear.txt")
	shakespeareWords := getWords(s, shakespeare)

	analysis := textio.Read(s, "analysis.csv")
	analysisRecords := parseAnalysis(s, analysis)

	trigger := trigger.AfterEndOfWindow().
		EarlyFiring(trigger.AfterProcessingTime().
			PlusDelay(5 * time.Second)).
		LateFiring(trigger.Repeat(trigger.AfterCount(1)))

	fixedWindowedItems := beam.WindowInto(s, window.NewFixedWindows(30*time.Second), shakespeareWords,
		beam.Trigger(trigger),
		beam.AllowedLateness(30*time.Second),
		beam.PanesDiscard(),
	)

	result := matchWords(s, fixedWindowedItems, analysisRecords)

	parts := partition(s, result)

	negativeWords := parts[0]
	//positiveWords := parts[1]

	negativeWordsCount1 := beam.ParDo(s, func(analysis2 Analysis, emit func(string)) { emit(analysis2.Word) }, negativeWords)
	debug.Print(s, stats.CountElms(s, negativeWordsCount1))

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

func parseAnalysis(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, func(line string, emit func(analysis Analysis)) {
		parts := strings.Split(line, ",")
		if parts[0] != "Word" {
			emit(Analysis{
				Word:         strings.ToLower(parts[0]),
				Negative:     parts[1],
				Positive:     parts[2],
				Uncertainty:  parts[3],
				Litigious:    parts[4],
				Strong:       parts[5],
				Weak:         parts[6],
				Constraining: parts[7],
			})
		}
	}, input)
}

func getWords(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, func(line string, emit func(string)) {
		for _, word := range wordRE.FindAllString(line, -1) {
			emit(strings.ToLower(word))
		}
	}, input)
}

func matchWords(s beam.Scope, input beam.PCollection, viewPCollection beam.PCollection) beam.PCollection {
	view := beam.SideInput{
		Input: viewPCollection,
	}
	return beam.ParDo(s, matchFn, input, view)
}

func matchFn(word string, view func(analysis *Analysis) bool, emit func(analysis Analysis)) {
	var newAnalysis Analysis
	for view(&newAnalysis) {
		if word == newAnalysis.Word {
			emit(newAnalysis)
		}
	}
}

func partition(s beam.Scope, input beam.PCollection) []beam.PCollection {
	return beam.Partition(s, 3, func(analysis Analysis) int {
		if analysis.Negative != "0" {
			return 0
		}
		if analysis.Positive != "0" {
			return 1
		}
		return 2
	}, input)
}
