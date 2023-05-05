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

//   beam-playground:
//     name: CoreTransformsChallenge2
//     description: Core Transforms second motivating challenge.
//     multifile: false
//     context_line: 48
//     categories:
//       - Quickstart
//     complexity: BASIC
//     tags:
//       - hellobeam

package main

import (
	"context"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/filter"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
	_ "strconv"
	_ "strings"
)

func less(a, b string) bool {
	return true
}

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

	input := textio.Read(s, "gs://apache-beam-samples/game/small/gaming_data.csv")

	lines := getLines(s, input)

	kvPCollection := getSplitLineAsMap(s, lines)

	combined := combine(s, kvPCollection)
	debug.Print(s, combined)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

func getLines(s beam.Scope, input beam.PCollection) beam.PCollection {
	return filter.Include(s, input, func(element string) bool {
		return element != ""
	})
}

func getSplitLineAsMap(s beam.Scope, input beam.PCollection) beam.PCollection {
	c := 0
	return beam.ParDo(s, func(line string, emit func(string, int)) {
		if c == 100 {
			return
		}
		c++
		emit("", 0)
	}, input)
}

func combine(s beam.Scope, input beam.PCollection) beam.PCollection {
	return input
}
