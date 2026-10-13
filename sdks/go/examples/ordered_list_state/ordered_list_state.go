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

// ordered_list_state is a toy pipeline demonstrating the use of OrderedListState.
// It creates keyed elements with timestamps, stores them in ordered list state,
// and reads back sub-ranges to emit summaries per key.
package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

// eventLogFn accumulates timestamped events per key using OrderedListState
// and emits a summary of events seen so far.
type eventLogFn struct {
	Events state.OrderedList[string]
}

func (fn *eventLogFn) ProcessElement(p state.Provider, key string, ts int64, emit func(string)) error {
	// Store an event using the input value as the sort key.
	event := fmt.Sprintf("event@%d", ts)
	fn.Events.Add(p, ts, event)

	// Read all events accumulated so far for this key.
	entries, ok, err := fn.Events.Read(p)
	if err != nil {
		return err
	}
	if ok {
		latest := entries[len(entries)-1]
		emit(fmt.Sprintf("key=%s count=%d latest=%s (sort_key=%d)", key, len(entries), latest.Value, latest.SortKey))
	}

	return nil
}

func init() {
	register.DoFn4x1[state.Provider, string, int64, func(string), error](&eventLogFn{})
	register.Emitter1[string]()
	register.Function1x2(toKeyed)
}

// toKeyed maps an integer to a KV pair of (key, timestamp).
func toKeyed(i int) (string, int64) {
	return fmt.Sprintf("user-%d", i%3), int64(i * 1000)
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

	// Create a small set of input elements.
	impulse := beam.CreateList(s, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

	// Key and timestamp each element.
	keyed := beam.ParDo(s, toKeyed, impulse)

	// Apply the stateful DoFn with OrderedListState.
	summaries := beam.ParDo(s, &eventLogFn{
		Events: state.MakeOrderedListState[string]("events"),
	}, keyed)

	debug.Print(s, summaries)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
