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

// Package snippets contains code used in the Beam Programming Guide
// as examples for the Apache Beam Go SDK. These snippets are compiled
// and their tests run to ensure correctness. However, due to their
// piecemeal pedagogical use, they may not be the best example of
// production code.
//
// The Beam Programming Guide can be found at https://beam.apache.org/documentation/programming-guide/.
package snippets

import (
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/teststream"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
)

// [START after_window_trigger]
trigger := window.TriggerAfterEndOfWindow()
					.EarlyFiring(window.TriggerAfterProcessingTime(60000))
					.LateFiring(window.TriggerRepeat(window.TriggerAfterCount(1)))
// [END after_window_trigger]

// TriggerAlways tests the Always trigger, it is expected to receive every input value as the output.
func TriggerAlways(s beam.Scope) beam.PCollection {
	con := teststream.NewConfig()
	con.AddElements(1000, 1.0, 2.0, 3.0)
	con.AdvanceWatermark(11000)
	pCollection := teststream.Create(s, con)
	windowSize := 10 * time.Second

	// [START always_trigger]
	// define an always trigger
	tr := window.Trigger{Kind: window.AlwaysTrigger}
	windowed := beam.WindowInto(s, window.NewFixedWindows(windowSize), pCollection, beam.WindowTrigger{Name: tr}, beam.AccumulationMode{Mode: window.Discarding})
	// [END always_trigger]
	sums := stats.Sum(s, windowed)
	return sums
}
