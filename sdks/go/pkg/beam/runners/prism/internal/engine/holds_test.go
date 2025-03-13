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
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
)

func TestHoldTracker(t *testing.T) {

	type op func(*holdTracker)
	add := func(hold mtime.Time, count int) op {
		return func(ht *holdTracker) {
			ht.Add(hold, count)
		}
	}

	drop := func(hold mtime.Time, count int) op {
		return func(ht *holdTracker) {
			ht.Drop(hold, count)
		}
	}

	tests := []struct {
		name    string
		ops     []op
		wantMin mtime.Time
		wantLen int
	}{
		{
			name:    "zero-max",
			wantMin: mtime.MaxTimestamp,
			wantLen: 0,
		}, {

			name: "one-min",
			ops: []op{
				add(mtime.MinTimestamp, 1),
			},
			wantMin: mtime.MinTimestamp,
			wantLen: 1,
		}, {

			name: "cleared-max",
			ops: []op{
				add(mtime.MinTimestamp, 1),
				drop(mtime.MinTimestamp, 1),
			},
			wantMin: mtime.MaxTimestamp,
			wantLen: 0,
		}, {
			name: "cleared-non-eogw",
			ops: []op{
				add(mtime.MinTimestamp, 1),
				add(mtime.EndOfGlobalWindowTime, 1),
				drop(mtime.MinTimestamp, 1),
			},
			wantMin: mtime.EndOfGlobalWindowTime,
			wantLen: 1,
		}, {
			name: "uncleared-non-min",
			ops: []op{
				add(mtime.MinTimestamp, 2),
				add(mtime.EndOfGlobalWindowTime, 1),
				drop(mtime.MinTimestamp, 1),
			},
			wantMin: mtime.MinTimestamp,
			wantLen: 2,
		}, {
			name: "uncleared-non-min",
			ops: []op{
				add(1, 1),
				add(2, 1),
				add(3, 1),
				drop(2, 1),
				add(4, 1),
				add(3, 1),
				drop(1, 1),
				add(2, 1),
				drop(4, 1),
			},
			wantMin: 2,
			wantLen: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tracker := newHoldTracker()
			for _, op := range test.ops {
				op(tracker)
			}
			if got, want := tracker.Min(), test.wantMin; got != want {
				t.Errorf("tracker.heap.Min() = %v, want %v", got, want)
			}
			if got, want := tracker.heap.Len(), test.wantLen; got != want {
				t.Errorf("tracker.heap.Len() = %v, want %v", got, want)
			}
		})
	}
}
