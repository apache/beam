// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package engine

import (
	"container/heap"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
)

// Notes on Processing Time handling:
//
// ProcessingTime events (processingTime timers, process continuations, triggers) necessarily need to operate on a global queue.
// However, PT timers are per key+family+tag, and may be overwritten by subsequent elements.
// So, similarly to event time timers, we need to manage a "last set" queue, and to manage the holds.
// This implies they should probably be handled by state, instead of globally.
// In reality, it's probably going to be "both", a global PT event queue, and per stage state.
//
// In principle, timers would be how to implement the related features, so getting those right will simplify their handling.
// Test stream is already central, but doesn't set events, it controls their execution.
//
// The ElementManager doesn't retain any data itself, so it should not hold material data about what is being triggered.
// The ElementManager should  only contain which stage state should be triggered when in a time domain.
//
// ProcessContinuations count as pending events, and must be drained accordingly before time expires.
//
// A stage may trigger on multiple ticks.
// It's up to a stage to schedule additional work on those notices.

// stageRefreshQueue manages ProcessingTime events, in particular, which stages need notification
// at which points in processing time they occur. It doesn't handle the interface between
// walltime or any synthetic notions of time.
//
// stageRefreshQueue is not goroutine safe and relies on external synchronization.
type stageRefreshQueue struct {
	events map[mtime.Time]set[string]
	order  mtimeHeap
}

// newStageRefreshQueue creates an initialized stageRefreshQueue.
func newStageRefreshQueue() *stageRefreshQueue {
	return &stageRefreshQueue{
		events: map[mtime.Time]set[string]{},
	}
}

// Schedule a stage event at the given time.
func (q *stageRefreshQueue) Schedule(t mtime.Time, stageID string) {
	if s, ok := q.events[t]; ok {
		// We already have a trigger at this time, mutate that instead.
		if s.present(stageID) {
			// We already notify this stage at this time, no action required.
			return
		}
		s.insert(stageID)
		return
	}
	q.events[t] = set[string]{stageID: struct{}{}}
	heap.Push(&q.order, t)
}

// Peek returns the minimum time in the queue and whether it is valid.
// If there are no times left in the queue, the boolean will be false.
func (q *stageRefreshQueue) Peek() (mtime.Time, bool) {
	if len(q.order) == 0 {
		return mtime.MaxTimestamp, false
	}
	return q.order[0], true
}

// AdvanceTo takes in the current now time, and returns the set of ids that need a refresh.
func (q *stageRefreshQueue) AdvanceTo(now mtime.Time) set[string] {
	notify := set[string]{}
	for {
		// If there are no elements, then we're done.
		if len(q.order) == 0 || q.order[0] > now {
			return notify
		}
		// pop elements off the queue until the next time is later than now.
		next := heap.Pop(&q.order).(mtime.Time)
		notify.merge(q.events[next])
		delete(q.events, next)
	}
}
