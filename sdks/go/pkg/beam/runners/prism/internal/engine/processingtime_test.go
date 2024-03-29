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
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/google/go-cmp/cmp"
)

func TestProcessingTimeQueue(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		q := newStageRefreshQueue()
		emptyTime, ok := q.Peek()
		if ok != false {
			t.Errorf("q.Peek() on empty queue should have returned false")
		}
		if got, want := emptyTime, mtime.MaxTimestamp; got != want {
			t.Errorf("q.Peek() on empty queue returned %v, want %v", got, want)
		}

		tests := []mtime.Time{
			mtime.MinTimestamp,
			-273,
			0,
			42,
			mtime.EndOfGlobalWindowTime,
			mtime.MaxTimestamp,
		}
		for _, test := range tests {
			if got, want := q.AdvanceTo(test), (set[string]{}); len(got) > 0 {
				t.Errorf("q.AdvanceTo(%v) on empty queue returned %v, want %v", test, got, want)
			}
		}
	})
	t.Run("scheduled", func(t *testing.T) {
		type event struct {
			t     mtime.Time
			stage string
		}

		s := func(ids ...string) set[string] {
			ret := set[string]{}
			for _, id := range ids {
				ret.insert(id)
			}
			return ret
		}

		tests := []struct {
			name   string
			events []event

			minTime mtime.Time

			advanceTime mtime.Time
			want        set[string]
		}{
			{
				"singleBefore",
				[]event{{1, "test1"}},
				1,
				0,
				s(),
			}, {
				"singleAt",
				[]event{{1, "test1"}},
				1,
				1,
				s("test1"),
			}, {
				"singleAfter",
				[]event{{1, "test1"}},
				1,
				2,
				s("test1"),
			}, {
				"trioDistinct",
				[]event{{1, "test1"}, {2, "test2"}, {3, "test3"}},
				1,
				2,
				s("test1", "test2"),
			}, {
				"trioDistinctReversed",
				[]event{{3, "test3"}, {2, "test2"}, {1, "test1"}},
				1,
				2,
				s("test1", "test2"),
			}, {
				"trioDistinctTimeSameId",
				[]event{{3, "test"}, {2, "test"}, {1, "test"}},
				1,
				2,
				s("test"),
			}, {
				"trioOneTime",
				[]event{{1, "test3"}, {1, "test2"}, {1, "test1"}},
				1,
				1,
				s("test1", "test2", "test3"),
			}, {
				"trioDuplicates",
				[]event{{1, "test"}, {1, "test"}, {1, "test"}},
				1,
				1,
				s("test", "test", "test"),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				q := newStageRefreshQueue()
				for _, e := range test.events {
					q.Schedule(e.t, e.stage)
				}
				if got, _ := q.Peek(); got != test.minTime {
					t.Errorf("q.Peek() = %v, want %v", got, test.minTime)
				}

				if got, want := q.AdvanceTo(test.advanceTime), test.want; !cmp.Equal(got, want) {
					t.Errorf("q.AdvanceTo(%v) = %v, want %v", test.advanceTime, got, want)
				}
			})
		}
	})
}
