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

package snippets

import (
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
)

func settingWindows(s beam.Scope, items beam.PCollection) {
	// [START setting_fixed_windows]
	fixedWindowedItems := beam.WindowInto(s,
		window.NewFixedWindows(60*time.Second),
		items)
	// [END setting_fixed_windows]

	// [START setting_sliding_windows]
	slidingWindowedItems := beam.WindowInto(s,
		window.NewSlidingWindows(5*time.Second, 30*time.Second),
		items)
	// [END setting_sliding_windows]

	// [START setting_session_windows]
	sessionWindowedItems := beam.WindowInto(s,
		window.NewSessions(600*time.Second),
		items)
	// [END setting_session_windows]

	// [START setting_global_window]
	globalWindowedItems := beam.WindowInto(s,
		window.NewGlobalWindows(),
		items)
	// [END setting_global_window]

	// [START setting_allowed_lateness]
	windowedItems := beam.WindowInto(s,
		window.NewFixedWindows(1*time.Minute), items,
		beam.AllowedLateness(2*24*time.Hour), // 2 days
	)
	// [END setting_allowed_lateness]

	_ = []beam.PCollection{
		fixedWindowedItems,
		slidingWindowedItems,
		sessionWindowedItems,
		globalWindowedItems,
		windowedItems,
	}
}

// LogEntry is a dummy type for documentation purposes.
type LogEntry int

func extractEventTime(LogEntry) time.Time {
	// Note: Returning time.Now() is always going to be processing time
	// not EventTime. For true event time, one needs to extract the
	// time from the element itself.
	return time.Now()
}

// [START setting_timestamp]

// AddTimestampDoFn extracts an event time from a LogEntry.
func AddTimestampDoFn(element LogEntry, emit func(beam.EventTime, LogEntry)) {
	et := extractEventTime(element)
	// Defining an emitter with beam.EventTime as the first parameter
	// allows the DoFn to set the event time for the emitted element.
	emit(mtime.FromTime(et), element)
}

// [END setting_timestamp]

func timestampedCollection(s beam.Scope, unstampedLogs beam.PCollection) {
	// [START setting_timestamp_pipeline]
	stampedLogs := beam.ParDo(s, AddTimestampDoFn, unstampedLogs)
	// [END setting_timestamp_pipeline]
	_ = stampedLogs
}
