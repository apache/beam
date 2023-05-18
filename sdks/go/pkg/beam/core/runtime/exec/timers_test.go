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

package exec

import (
	"bytes"
	"sort"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/timers"
	"github.com/google/go-cmp/cmp"
)

func equalTimers(a, b TimerRecv) bool {
	return a.Key.Elm == b.Key.Elm && a.Tag == b.Tag && (a.FireTimestamp) == b.FireTimestamp && a.Clear == b.Clear
}

func TestTimerEncodingDecoding(t *testing.T) {
	tc := coder.NewT(coder.NewString(), window.NewGlobalWindows().Coder())
	ec := MakeElementEncoder(coder.SkipW(tc))
	dec := MakeElementDecoder(coder.SkipW(tc))

	tests := []struct {
		name   string
		tm     TimerRecv
		result bool
	}{
		{
			name: "all set fields",
			tm: TimerRecv{
				Key: &FullValue{Elm: "Basic"},
				TimerMap: timers.TimerMap{
					Tag:           "first",
					Clear:         false,
					FireTimestamp: mtime.Now(),
				},
				Windows: window.SingleGlobalWindow,
			},
			result: true,
		},
		{
			name: "without tag",
			tm: TimerRecv{
				Key: &FullValue{Elm: "Basic"},
				TimerMap: timers.TimerMap{
					Tag:           "",
					Clear:         false,
					FireTimestamp: mtime.Now(),
				},
				Windows: window.SingleGlobalWindow,
			},
			result: true,
		},
		{
			name: "with clear set",
			tm: TimerRecv{
				Key: &FullValue{Elm: "Basic"},
				TimerMap: timers.TimerMap{
					Tag:           "first",
					Clear:         true,
					FireTimestamp: mtime.Now(),
				},
				Windows: window.SingleGlobalWindow,
			},
			result: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fv := FullValue{Elm: test.tm}
			var buf bytes.Buffer
			err := ec.Encode(&fv, &buf)
			if err != nil {
				t.Fatalf("error encoding timer: %#v, got: %v", test.tm, err)
			}

			gotFv, err := dec.Decode(&buf)
			if err != nil {
				t.Fatalf("failed to decode timer, got %v", err)
			}

			if got, want := gotFv.Elm.(TimerRecv), test.tm; test.result != equalTimers(got, want) {
				t.Errorf("got timer %v, want %v", got, want)
			}
		})
	}

}

func TestSortableTimer_Less(t *testing.T) {
	f := "family"

	now := mtime.FromTime(time.Now())

	baseTimer := sortableTimer{
		Domain: timers.EventTimeDomain,
		TimerMap: timers.TimerMap{
			Family:        f,
			Tag:           "",
			Clear:         false,
			FireTimestamp: now,
			HoldTimestamp: now,
		},
	}
	eventTimer := baseTimer
	processingTimer := baseTimer
	processingTimer.Domain = timers.ProcessingTimeDomain

	clearedTimer := baseTimer
	clearedTimer.Clear = true

	lesserFireTimer := baseTimer
	lesserFireTimer.FireTimestamp -= 10
	greaterFireTimer := baseTimer
	greaterFireTimer.FireTimestamp += 10

	lesserHoldTimer := baseTimer
	lesserHoldTimer.HoldTimestamp -= 10
	greaterHoldTimer := baseTimer
	greaterHoldTimer.HoldTimestamp += 10

	leastTagTimer := baseTimer

	lesserTagTimer := baseTimer
	lesserTagTimer.Tag = "Bar"

	greaterTagTimer := baseTimer
	greaterTagTimer.Tag = "Foo"

	tests := []struct {
		name        string
		left, right sortableTimer
		want        bool
	}{
		{
			name: "equal ",
			left: baseTimer, right: baseTimer,
			want: false,
		}, {
			name: "processing time lesser",
			left: processingTimer, right: eventTimer,
			want: true,
		}, {
			name: "event time greater",
			left: eventTimer, right: processingTimer,
			want: false,
		}, {
			name: "cleared lesser",
			left: clearedTimer, right: baseTimer,
			want: true,
		}, {
			name: "uncleared greater",
			left: baseTimer, right: clearedTimer,
			want: false,
		}, {
			name: "greater firing time",
			left: baseTimer, right: greaterFireTimer,
			want: true,
		}, {
			name: "lesser firing time",
			left: baseTimer, right: lesserFireTimer,
			want: false,
		}, {
			name: "greater hold time",
			left: baseTimer, right: greaterHoldTimer,
			want: true,
		}, {
			name: "lesser hold time",
			left: baseTimer, right: lesserHoldTimer,
			want: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.left.Less(test.right) != test.want {
				t.Errorf("(%+v).Less(%+v) not true", test.left, test.right)
			}
		})
	}

	t.Run("sorted", func(t *testing.T) {
		wantedOrder := timerHeap{
			processingTimer,
			clearedTimer,
			lesserFireTimer,
			lesserHoldTimer,
			leastTagTimer, // equal to base
			baseTimer,     // basic version of everything.
			eventTimer,    //equal to base
			lesserTagTimer,
			greaterTagTimer,
			greaterHoldTimer,
			greaterFireTimer,
		}
		if sort.IsSorted(wantedOrder) {
			return // success!
		}
		for i, v := range wantedOrder[1:] {
			left, right := wantedOrder[i], v
			if !left.Less(right) && left != right {
				t.Errorf("%v \n%+v not < \n%+v", i, left, right)
			}
		}
	})

}

func TestTimerHeap_HeadSetIter(t *testing.T) {
	f := "family"

	now := mtime.FromTime(time.Now())

	nextTimer := sortableTimer{
		Domain: timers.EventTimeDomain,
		TimerMap: timers.TimerMap{
			Family:        f,
			Tag:           "",
			Clear:         false,
			FireTimestamp: now,
			HoldTimestamp: now,
		},
	}
	lesserFireTimer := nextTimer
	lesserFireTimer.FireTimestamp -= 10
	greaterFireTimer := nextTimer
	greaterFireTimer.FireTimestamp += 10

	tests := []struct {
		name    string
		inserts []sortableTimer
		key     sortableTimer
		want    []sortableTimer
	}{
		{
			name:    "empty",
			inserts: nil,
			key:     nextTimer,
			want:    nil,
		},
		{
			name:    "single-Greater",
			inserts: []sortableTimer{greaterFireTimer},
			key:     nextTimer,
			want:    nil,
		},
		{
			name:    "single-Equal",
			inserts: []sortableTimer{nextTimer},
			key:     nextTimer,
			want:    []sortableTimer{nextTimer},
		},
		{
			name:    "single-Lesser",
			inserts: []sortableTimer{lesserFireTimer},
			key:     nextTimer,
			want:    []sortableTimer{lesserFireTimer},
		},
		{
			name:    "lessthan or equal",
			inserts: []sortableTimer{lesserFireTimer, nextTimer, greaterFireTimer},
			key:     nextTimer,
			want:    []sortableTimer{lesserFireTimer, nextTimer},
		},
		{
			name:    "lessthan or equal- different order",
			inserts: []sortableTimer{greaterFireTimer, lesserFireTimer, nextTimer},
			key:     nextTimer,
			want:    []sortableTimer{lesserFireTimer, nextTimer},
		},
	}

	// Test inserting everything at the same time.
	for _, test := range tests {
		t.Run("singlebatch_"+test.name, func(t *testing.T) {
			var h timerHeap
			for _, timer := range test.inserts {
				h.Add(timer)
			}
			iter := h.HeadSetIter(test.key)

			var got []sortableTimer
			for {
				if v, ok := iter(); ok {
					got = append(got, v)
				} else {
					break
				}
			}

			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("h.HeadSet(%+v) diff (-want, +got):\n%v", test.key, diff)
			}
		})
	}

	// Test pulling after every insert, to validate the iterator is a dynamic view that reflects changes.
	for _, test := range tests {
		t.Run("dynamic_"+test.name, func(t *testing.T) {
			var h timerHeap

			iter := h.HeadSetIter(test.key)
			var got []sortableTimer
			for _, timer := range test.inserts {
				h.Add(timer)

				if v, ok := iter(); ok {
					got = append(got, v)
				}
			}

			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("h.HeadSet(%+v) diff (-want, +got):\n%v", test.key, diff)
			}
		})
	}
}
