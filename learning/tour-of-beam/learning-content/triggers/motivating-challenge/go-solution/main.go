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
/*
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

// beam-playground:
//   name: TriggersSolution
//   description: TriggersSolution example.
//   multifile: false
//   context_line: 46
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

package main

import (
	"context"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window/trigger"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"strconv"
	"strings"
	"time"
)

func main() {
	p, s := beam.NewPipelineWithRoot()

	input := textio.Read(s, "gs://apache-beam-samples/nyc_taxi/misc/sample1000.csv")

	// Extract cost from PCollection
	cost := ExtractCostFromFile(s, input)

	trigger := trigger.AfterAll([]trigger.Trigger{trigger.AfterCount(10), trigger.AfterEndOfWindow().
		EarlyFiring(trigger.AfterProcessingTime().
			PlusDelay(60 * time.Second)).
		LateFiring(trigger.Repeat(trigger.AfterCount(1)))})

	fixedWindowedItems := beam.WindowInto(s, window.NewFixedWindows(60*time.Second), cost, beam.Trigger(trigger), beam.PanesDiscard())

	output(s, fixedWindowedItems)

	err := beamx.Run(context.Background(), p)
	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

func ExtractCostFromFile(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, func(line string) float64 {
		taxi := strings.Split(strings.TrimSpace(line), ",")
		if len(taxi) > 16 {
			cost, _ := strconv.ParseFloat(taxi[16], 64)
			return cost
		}
		return 0.0
	}, input)
}

func output(s beam.Scope, input beam.PCollection) {
	beam.ParDo0(s, func(element interface{}) {
		fmt.Println(element)
	}, input)
}
