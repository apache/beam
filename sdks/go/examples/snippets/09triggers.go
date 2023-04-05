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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window/trigger"
)

func TriggerAfterEndOfWindow(s beam.Scope, pCollection beam.PCollection) {
	// [START after_window_trigger]
	trigger := trigger.AfterEndOfWindow().
		EarlyFiring(trigger.AfterProcessingTime().
			PlusDelay(60 * time.Second)).
		LateFiring(trigger.Repeat(trigger.AfterCount(1)))
	// [END after_window_trigger]
	beam.WindowInto(s, window.NewFixedWindows(10*time.Second), pCollection, beam.Trigger(trigger), beam.PanesDiscard())
}

func TriggerAlways(s beam.Scope, pCollection beam.PCollection) {
	// [START always_trigger]
	beam.WindowInto(s, window.NewFixedWindows(10*time.Second), pCollection,
		beam.Trigger(trigger.Always()),
		beam.PanesDiscard(),
	)
	// [END always_trigger]
}

func ComplexTriggers(s beam.Scope, pcollection beam.PCollection) {
	// [START setting_a_trigger]
	windowedItems := beam.WindowInto(s,
		window.NewFixedWindows(1*time.Minute), pcollection,
		beam.Trigger(trigger.AfterProcessingTime().
			PlusDelay(1*time.Minute)),
		beam.AllowedLateness(30*time.Minute),
		beam.PanesDiscard(),
	)
	// [END setting_a_trigger]

	// [START setting_allowed_lateness]
	allowedToBeLateItems := beam.WindowInto(s,
		window.NewFixedWindows(1*time.Minute), pcollection,
		beam.Trigger(trigger.AfterProcessingTime().
			PlusDelay(1*time.Minute)),
		beam.AllowedLateness(30*time.Minute),
	)
	// [END setting_allowed_lateness]

	// [START model_composite_triggers]
	compositeTriggerItems := beam.WindowInto(s,
		window.NewFixedWindows(1*time.Minute), pcollection,
		beam.Trigger(trigger.AfterEndOfWindow().
			LateFiring(trigger.AfterProcessingTime().
				PlusDelay(10*time.Minute))),
		beam.AllowedLateness(2*24*time.Hour),
	)
	// [END model_composite_triggers]

	// TODO(BEAM-3304) AfterAny is not yet implemented.
	// Implement so the following compiles when no longer commented out.

	// [START other_composite_trigger]
	// beam.Trigger(
	// 	trigger.TriggerAfterAny(
	// 		trigger.AfterCount(100),
	// 		trigger.AfterProcessingTime().
	// 			PlusDelay(1*time.Minute)),
	// )
	// [END other_composite_trigger]

	_ = []beam.PCollection{
		windowedItems,
		allowedToBeLateItems,
		compositeTriggerItems,
	}
}
