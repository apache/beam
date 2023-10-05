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
//   name: FinalChallenge2
//   description: Final challenge 2.
//   multifile: true
//   files:
//     - name: analysis.csv
//   context_line: 54
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

package main

import (
	"context"
	"log"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

func main() {

	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	shakespeare := textio.Read(s, "gs://apache-beam-samples/shakespeare/kinglear.txt")

	debug.Print(s, getWords(s, shakespeare))

	err := beamx.Run(context.Background(), p)
	if err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

func getWords(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, func(line string, emit func(string)) {
		c := strings.Split(strings.ToLower(line), " ")
		for _, word := range c {
			emit(word)
		}
	}, input)
}
