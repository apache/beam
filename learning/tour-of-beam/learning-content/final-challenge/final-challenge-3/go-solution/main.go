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
//   name: FinalSolution3
//   description: Final challenge solution 3.
//   multifile: false
//   context_line: 54
//   categories:
//     - Quickstart
//   complexity: BASIC
//   tags:
//     - hellobeam

package main

import (
	"context"
	"log"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
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

func main() {
	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	shakespeare := textio.Read(s, "gs://apache-beam-samples/shakespeare/kinglear.txt")

	analysis := textio.Read(s, "analysis.csv")
	analysisRecords := beam.ParDo(s, parseAnalysis, analysis)

	shakespeareWords := beam.ParDo(s, func(line string) string {
		// Simplified text preprocessing.
		words := strings.Fields(line)
		return strings.ToLower(words[0])
	}, shakespeare)

	matchedWords := beam.ParDo(s, func(word string, analysisIter func(*Analysis) bool) (*Analysis, error) {
		var analysis Analysis
		if !analysisIter(&analysis) {
			return nil, nil // Skip if no analysis.
		}
		return &analysis, nil
	}, shakespeareWords, beam.SideInput{Input: analysisRecords})

	// Just counting all words in this simplified example.
	counted := stats.Count(s, matchedWords)

	textio.Write(s, "output.txt", counted)

	err := beam.Run(context.Background(), "direct", p)
	if err != nil {
		log.Fatalf(ctx, "Failed to execute job: %v", err)
	}
}

func parseAnalysis(line string) (Analysis, error) {
	// Implement CSV parsing here.
	// Assuming comma-separated values for simplicity.
	parts := strings.Split(line, ",")
	return Analysis{
		Word:         parts[0],
		Negative:     parts[1],
		Positive:     parts[2],
		Uncertainty:  parts[3],
		Litigious:    parts[4],
		Strong:       parts[5],
		Weak:         parts[6],
		Constraining: parts[7],
	}, nil
}
