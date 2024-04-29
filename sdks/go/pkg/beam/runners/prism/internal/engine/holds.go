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

package engine

import (
	"container/heap"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
)

// holdHeap orders holds based on their timestamps
// so we can always find the minimum timestamp of pending holds.
type holdHeap []mtime.Time

func (h holdHeap) Len() int           { return len(h) }
func (h holdHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h holdHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *holdHeap) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(mtime.Time))
}

func (h *holdHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// holdTracker track the watermark holds for a stage.
//
// Timers hold back the watermark until they fire, but multiple
// timers may set the same watermark hold.
// To track when the watermark may advance further this structure maintains
// counts for each set watermark hold.
// As timers are processed, their associated holds are removed, reducing the counts.
//
// A heap of the hold times is kept so we have quick access to the minimum hold, for calculating
// how to advance the watermark.
type holdTracker struct {
	heap   holdHeap
	counts map[mtime.Time]int
}

func newHoldTracker() *holdTracker {
	return &holdTracker{
		counts: map[mtime.Time]int{},
	}
}

// Drop the given hold count. When the count of a hold time reaches zero, it's
// removed from the heap. Drop panics if holds become negative.
func (ht *holdTracker) Drop(hold mtime.Time, v int) {
	n := ht.counts[hold] - v
	if n > 0 {
		ht.counts[hold] = n
		return
	} else if n < 0 {
		panic(fmt.Sprintf("prism error: negative watermark hold count %v for time %v", n, hold))
	}
	delete(ht.counts, hold)
	for i, h := range ht.heap {
		if hold == h {
			heap.Remove(&ht.heap, i)
			break
		}
	}
}

// Add a hold a number of times to heap. If the hold time isn't already present in the heap, it is added.
func (ht *holdTracker) Add(hold mtime.Time, v int) {
	// Mark the hold in the heap.
	ht.counts[hold] = ht.counts[hold] + v

	if len(ht.counts) != len(ht.heap) {
		// Since there's a difference, the hold should not be in the heap, so we add it.
		heap.Push(&ht.heap, hold)
	}
}

// Min returns the earliest hold in the heap. Returns [mtime.MaxTimestamp] if the heap is empty.
func (ht *holdTracker) Min() mtime.Time {
	minWatermarkHold := mtime.MaxTimestamp
	if len(ht.heap) > 0 {
		minWatermarkHold = ht.heap[0]
	}
	return minWatermarkHold
}
