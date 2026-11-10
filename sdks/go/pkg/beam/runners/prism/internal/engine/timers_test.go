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
	"slices"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/google/go-cmp/cmp"
)

func TestTimerHandler(t *testing.T) {
	fireTime1, fireTime2, fireTime3 := mtime.FromMilliseconds(1000), mtime.FromMilliseconds(1100), mtime.FromMilliseconds(1200)
	eventTime1, eventTime2, eventTime3 := mtime.FromMilliseconds(200), mtime.FromMilliseconds(210), mtime.FromMilliseconds(220)
	holdTime1, holdTime2, holdTime3 := mtime.FromMilliseconds(300), mtime.FromMilliseconds(301), mtime.FromMilliseconds(303)
	tid := "testtransform"
	family := "testfamily"
	userKey1, userKey2, userKey3 := []byte("userKey1"), []byte("userKey2"), []byte("userKey3")
	glo := window.SingleGlobalWindow[0]
	iw1, iw2, iw3 := window.IntervalWindow{End: fireTime1}, window.IntervalWindow{End: fireTime2}, window.IntervalWindow{End: fireTime3}

	elemTagWin := func(userKey []byte, eventTime, holdTime mtime.Time, tag string, window typex.Window) element {
		return element{
			window:        window,
			timestamp:     eventTime,
			holdTimestamp: holdTime,
			pane:          typex.NoFiringPane(), // TODO, do something with pane.
			transform:     tid,
			family:        family,
			tag:           tag,
			keyBytes:      userKey,
		}
	}

	elem := func(userKey []byte, eventTime, holdTime mtime.Time) element {
		return elemTagWin(userKey, eventTime, holdTime, "", glo)
	}

	fireElem := func(fire mtime.Time, userKey []byte, eventTime, holdTime mtime.Time) fireElement {
		return fireElement{
			firing: fire,
			timer:  elem(userKey, eventTime, holdTime),
		}
	}

	tests := []struct {
		name   string
		insert []fireElement

		wantHolds map[mtime.Time]int

		onFire     mtime.Time
		wantTimers []element
	}{{
		name:   "noTimers",
		insert: []fireElement{},

		wantHolds:  map[mtime.Time]int{},
		onFire:     fireTime1,
		wantTimers: nil,
	}, {
		name:   "singleTimer-singleKey",
		insert: []fireElement{fireElem(fireTime1, userKey1, eventTime1, holdTime1)},

		wantHolds: map[mtime.Time]int{
			holdTime1: 1,
		},
		onFire:     fireTime1,
		wantTimers: []element{elem(userKey1, eventTime1, holdTime1)},
	}, {
		name: "singleTimer-multipleKeys",
		insert: []fireElement{
			fireElem(fireTime1, userKey1, eventTime1, holdTime1),
			fireElem(fireTime1, userKey2, eventTime1, holdTime1),
			fireElem(fireTime1, userKey3, eventTime1, holdTime1),
		},
		wantHolds: map[mtime.Time]int{
			holdTime1: 3,
		},
		onFire: fireTime1,

		wantTimers: []element{
			elem(userKey1, eventTime1, holdTime1),
			elem(userKey2, eventTime1, holdTime1),
			elem(userKey3, eventTime1, holdTime1),
		},
	}, {
		name: "multipleTimers-holdsChange",
		insert: []fireElement{
			fireElem(fireTime1, userKey1, eventTime1, holdTime1),
			fireElem(fireTime1, userKey1, eventTime2, holdTime2),
			fireElem(fireTime1, userKey1, eventTime3, holdTime3),
		},
		wantHolds: map[mtime.Time]int{
			holdTime3: 1,
		},
		onFire: fireTime3,
		wantTimers: []element{
			elem(userKey1, eventTime3, holdTime3),
		},
	}, {
		name: "multipleTimers-firesChange-nofire",
		insert: []fireElement{
			fireElem(fireTime1, userKey1, eventTime1, holdTime1),
			fireElem(fireTime2, userKey1, eventTime2, holdTime2),
			fireElem(fireTime3, userKey1, eventTime3, holdTime3),
		},
		wantHolds: map[mtime.Time]int{
			holdTime3: 1,
		},
		onFire:     fireTime1,
		wantTimers: nil, // Nothing should fire.
	}, {
		name: "multipleTimerKeys-firesAll",
		insert: []fireElement{
			fireElem(fireTime1, userKey1, eventTime1, holdTime1),
			fireElem(fireTime2, userKey2, eventTime2, holdTime2),
			fireElem(fireTime3, userKey3, eventTime3, holdTime3),
		},
		wantHolds: map[mtime.Time]int{
			holdTime1: 1,
			holdTime2: 1,
			holdTime3: 1,
		},
		onFire: fireTime3,
		wantTimers: []element{
			elem(userKey1, eventTime1, holdTime1),
			elem(userKey2, eventTime2, holdTime2),
			elem(userKey3, eventTime3, holdTime3),
		},
	}, {
		name: "multipleTimerKeys-firesTwo",
		insert: []fireElement{
			fireElem(fireTime3, userKey3, eventTime3, holdTime3),
			fireElem(fireTime2, userKey2, eventTime2, holdTime2),
			fireElem(fireTime1, userKey1, eventTime1, holdTime1),
		},
		wantHolds: map[mtime.Time]int{
			holdTime1: 1,
			holdTime2: 1,
			holdTime3: 1,
		},
		onFire: fireTime2,
		wantTimers: []element{
			elem(userKey1, eventTime1, holdTime1),
			elem(userKey2, eventTime2, holdTime2),
		},
	}, {
		name: "multipleTimerKeys-multiple-replacements",
		insert: []fireElement{
			fireElem(fireTime3, userKey3, eventTime3, holdTime3),
			fireElem(fireTime2, userKey2, eventTime2, holdTime2),
			fireElem(fireTime1, userKey1, eventTime1, holdTime1),

			fireElem(fireTime1, userKey3, eventTime3, holdTime1), // last userKey3 - present at fireTime2
			fireElem(fireTime2, userKey2, eventTime1, holdTime1),
			fireElem(fireTime3, userKey1, eventTime1, holdTime1),

			fireElem(fireTime3, userKey1, eventTime3, holdTime3),
			fireElem(fireTime2, userKey2, eventTime2, holdTime2),
			fireElem(fireTime1, userKey1, eventTime2, holdTime2),

			fireElem(fireTime2, userKey2, eventTime1, holdTime3),
			fireElem(fireTime3, userKey2, eventTime2, holdTime2), // last userkey2 - not present at fireTime2
			fireElem(fireTime1, userKey1, eventTime1, holdTime3), // last userkey1 - present at fireTime2
		},
		wantHolds: map[mtime.Time]int{
			holdTime1: 1,
			holdTime2: 1,
			holdTime3: 1,
		},
		onFire: fireTime2,
		wantTimers: []element{
			elem(userKey3, eventTime3, holdTime1),
			elem(userKey1, eventTime1, holdTime3),
		},
	}, {
		name: "multipleTimerTags-firesAll",
		insert: []fireElement{
			{firing: fireTime1, timer: elemTagWin(userKey1, eventTime1, holdTime1, "tag1", glo)},
			{firing: fireTime2, timer: elemTagWin(userKey1, eventTime1, holdTime1, "tag2", glo)},
			{firing: fireTime3, timer: elemTagWin(userKey1, eventTime1, holdTime1, "tag3", glo)},

			// Validate replacements on tags
			{firing: fireTime1, timer: elemTagWin(userKey1, eventTime2, holdTime1, "tag1", glo)},
			{firing: fireTime2, timer: elemTagWin(userKey1, eventTime2, holdTime1, "tag2", glo)},
			{firing: fireTime3, timer: elemTagWin(userKey1, eventTime2, holdTime1, "tag3", glo)},
		},
		wantHolds: map[mtime.Time]int{
			holdTime1: 3,
		},
		onFire: fireTime3,
		wantTimers: []element{
			elemTagWin(userKey1, eventTime2, holdTime1, "tag1", glo),
			elemTagWin(userKey1, eventTime2, holdTime1, "tag2", glo),
			elemTagWin(userKey1, eventTime2, holdTime1, "tag3", glo),
		},
	}, {
		name: "multipleTimerTags-firesAll",
		insert: []fireElement{
			{firing: fireTime1, timer: elemTagWin(userKey1, eventTime1, holdTime1, "", iw1)},
			{firing: fireTime2, timer: elemTagWin(userKey1, eventTime1, holdTime1, "", iw2)},
			{firing: fireTime3, timer: elemTagWin(userKey1, eventTime1, holdTime1, "", iw3)},

			// Validate replacements on windows
			{firing: fireTime1, timer: elemTagWin(userKey1, eventTime2, holdTime1, "", iw1)},
			{firing: fireTime2, timer: elemTagWin(userKey1, eventTime2, holdTime1, "", iw2)},
			{firing: fireTime3, timer: elemTagWin(userKey1, eventTime2, holdTime1, "", iw3)},
		},
		wantHolds: map[mtime.Time]int{
			holdTime1: 3,
		},
		onFire: fireTime3,
		wantTimers: []element{
			elemTagWin(userKey1, eventTime2, holdTime1, "", iw1),
			elemTagWin(userKey1, eventTime2, holdTime1, "", iw2),
			elemTagWin(userKey1, eventTime2, holdTime1, "", iw3),
		},
	},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			th := newTimerHandler()

			holdChanges := map[mtime.Time]int{}

			for _, ft := range test.insert {
				if ft.timer.IsData() {
					t.Fatalf("generated bad timer: %+v", ft)
				}
				th.Persist(ft.firing, ft.timer, holdChanges)
			}

			if d := cmp.Diff(test.wantHolds, holdChanges, cmp.AllowUnexported(element{}, fireElement{})); d != "" {
				t.Errorf("Persist(): diff (-want,+got):\n%v", d)
			}
			fired := th.FireAt(test.onFire)

			lessElement := func(a, b element) int {
				if a.timestamp < b.timestamp {
					return -1
				} else if a.timestamp > b.timestamp {
					return 1
				}
				if a.holdTimestamp < b.holdTimestamp {
					return -1
				} else if a.holdTimestamp > b.holdTimestamp {
					return 1
				}
				if string(a.keyBytes) < string(b.keyBytes) {
					return -1
				} else if string(a.keyBytes) > string(b.keyBytes) {
					return 1
				}
				if a.tag < b.tag {
					return -1
				} else if a.tag > b.tag {
					return 1
				}
				if a.window.MaxTimestamp() < b.window.MaxTimestamp() {
					return -1
				} else if a.window.MaxTimestamp() > b.window.MaxTimestamp() {
					return 1
				}
				return 0
			}
			slices.SortFunc(fired, lessElement)
			slices.SortFunc(test.wantTimers, lessElement)
			if d := cmp.Diff(test.wantTimers, fired, cmp.AllowUnexported(element{})); d != "" {
				t.Errorf("FireAt(%v): diff (-want,+got):\n%v", test.onFire, d)
			}
		})
	}
}
