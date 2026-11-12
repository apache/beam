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

func TestTimerHandler_Clear(t *testing.T) {
	fireTime1, fireTime2, fireTime3 := mtime.FromMilliseconds(1000), mtime.FromMilliseconds(1100), mtime.FromMilliseconds(1200)
	eventTime1, eventTime2 := mtime.FromMilliseconds(200), mtime.FromMilliseconds(210)
	holdTime1, holdTime2 := mtime.FromMilliseconds(300), mtime.FromMilliseconds(301)
	const (
		tid    = "testtransform"
		family = "testfamily"
	)
	userKey1, userKey2 := []byte("userKey1"), []byte("userKey2")
	glo := window.SingleGlobalWindow[0]

	setElem := func(userKey []byte, eventTime, holdTime mtime.Time, tag string, win typex.Window) element {
		return element{
			window:        win,
			timestamp:     eventTime,
			holdTimestamp: holdTime,
			pane:          typex.NoFiringPane(),
			transform:     tid,
			family:        family,
			tag:           tag,
			keyBytes:      userKey,
		}
	}

	clearElem := func(userKey []byte, tag string, win typex.Window) element {
		return element{
			window:    win,
			transform: tid,
			family:    family,
			tag:       tag,
			keyBytes:  userKey,
			sequence:  -1,
		}
	}

	t.Run("clearPendingTimer", func(t *testing.T) {
		th := newTimerHandler()
		holds := map[mtime.Time]int{}
		if mod := th.Persist(fireTime1, setElem(userKey1, eventTime1, holdTime1, "", glo), holds); mod != 1 {
			t.Fatalf("Persist(set) = %d, want 1", mod)
		}
		if got := th.Peek(); got != fireTime1 {
			t.Fatalf("Peek() after set = %v, want %v", got, fireTime1)
		}

		clearHolds := map[mtime.Time]int{}
		if mod := th.Persist(0, clearElem(userKey1, "", glo), clearHolds); mod != -1 {
			t.Errorf("Persist(clear) = %d, want -1", mod)
		}
		if got := th.Peek(); got != mtime.MaxTimestamp {
			t.Errorf("Peek() after clear = %v, want %v", got, mtime.MaxTimestamp)
		}
		if d := cmp.Diff(map[mtime.Time]int{holdTime1: -1}, clearHolds); d != "" {
			t.Errorf("clearHolds diff (-want,+got):\n%s", d)
		}
		if fired := th.FireAt(fireTime3); fired != nil {
			t.Errorf("FireAt(%v) = %v, want nil", fireTime3, fired)
		}
		if len(th.nextFiring) != 0 || len(th.toFire) != 0 || th.order.Len() != 0 {
			t.Errorf("handler not empty after clear: nextFiring=%d toFire=%d order=%d", len(th.nextFiring), len(th.toFire), th.order.Len())
		}
	})

	t.Run("clearPendingTimerInSameHoldMap", func(t *testing.T) {
		th := newTimerHandler()
		holds := map[mtime.Time]int{}
		if mod := th.Persist(fireTime1, setElem(userKey1, eventTime1, holdTime1, "", glo), holds); mod != 1 {
			t.Fatalf("Persist(set) = %d, want 1", mod)
		}
		if mod := th.Persist(0, clearElem(userKey1, "", glo), holds); mod != -1 {
			t.Errorf("Persist(clear) = %d, want -1", mod)
		}
		if d := cmp.Diff(map[mtime.Time]int{}, holds); d != "" {
			t.Errorf("holds diff (-want,+got):\n%s", d)
		}
	})

	t.Run("clearUnsetTimer", func(t *testing.T) {
		th := newTimerHandler()
		holds := map[mtime.Time]int{}
		if mod := th.Persist(0, clearElem(userKey1, "", glo), holds); mod != 0 {
			t.Errorf("Persist(clear unset) = %d, want 0", mod)
		}
		if got := th.Peek(); got != mtime.MaxTimestamp {
			t.Errorf("Peek() = %v, want %v", got, mtime.MaxTimestamp)
		}
		if len(holds) != 0 {
			t.Errorf("holds = %v, want empty", holds)
		}
		if len(th.nextFiring) != 0 {
			t.Errorf("nextFiring allocated for unset key: %v", th.nextFiring)
		}
		if fired := th.FireAt(fireTime3); fired != nil {
			t.Errorf("FireAt(%v) = %v, want nil", fireTime3, fired)
		}

		// Clearing an unset tag on a key that has another tag leaves the set tag intact.
		want := setElem(userKey1, eventTime1, holdTime1, "tag1", glo)
		if mod := th.Persist(fireTime1, want, holds); mod != 1 {
			t.Fatalf("Persist(set tag1) = %d, want 1", mod)
		}
		clearTagHolds := map[mtime.Time]int{}
		if mod := th.Persist(0, clearElem(userKey1, "tag2", glo), clearTagHolds); mod != 0 {
			t.Errorf("Persist(clear unset tag2) = %d, want 0", mod)
		}
		if len(clearTagHolds) != 0 {
			t.Errorf("clearTagHolds = %v, want empty", clearTagHolds)
		}
		if got := th.Peek(); got != fireTime1 {
			t.Errorf("Peek() = %v, want %v", got, fireTime1)
		}
		if d := cmp.Diff([]element{want}, th.FireAt(fireTime1), cmp.AllowUnexported(element{})); d != "" {
			t.Errorf("FireAt(%v) diff (-want,+got):\n%s", fireTime1, d)
		}
	})

	t.Run("clearOneOfTwoSameFiringSameKey", func(t *testing.T) {
		th := newTimerHandler()
		holds := map[mtime.Time]int{}
		t1 := setElem(userKey1, eventTime1, holdTime1, "tag1", glo)
		t2 := setElem(userKey1, eventTime2, holdTime2, "tag2", glo)
		th.Persist(fireTime1, t1, holds)
		th.Persist(fireTime1, t2, holds)

		if mod := th.Persist(0, clearElem(userKey1, "tag1", glo), holds); mod != -1 {
			t.Errorf("Persist(clear tag1) = %d, want -1", mod)
		}
		if got := th.Peek(); got != fireTime1 {
			t.Errorf("Peek() = %v, want %v", got, fireTime1)
		}
		if d := cmp.Diff(map[mtime.Time]int{holdTime2: 1}, holds); d != "" {
			t.Errorf("holds diff (-want,+got):\n%s", d)
		}
		if d := cmp.Diff([]element{t2}, th.FireAt(fireTime1), cmp.AllowUnexported(element{})); d != "" {
			t.Errorf("FireAt(%v) diff (-want,+got):\n%s", fireTime1, d)
		}
		if got := th.Peek(); got != mtime.MaxTimestamp {
			t.Errorf("Peek() after FireAt = %v, want %v", got, mtime.MaxTimestamp)
		}
	})

	t.Run("clearOneOfTwoSameFiringDifferentKeys", func(t *testing.T) {
		th := newTimerHandler()
		holds := map[mtime.Time]int{}
		t1 := setElem(userKey1, eventTime1, holdTime1, "", glo)
		t2 := setElem(userKey2, eventTime2, holdTime2, "", glo)
		th.Persist(fireTime1, t1, holds)
		th.Persist(fireTime1, t2, holds)

		if mod := th.Persist(0, clearElem(userKey1, "", glo), holds); mod != -1 {
			t.Errorf("Persist(clear userKey1) = %d, want -1", mod)
		}
		if got := th.Peek(); got != fireTime1 {
			t.Errorf("Peek() = %v, want %v", got, fireTime1)
		}
		if d := cmp.Diff(map[mtime.Time]int{holdTime2: 1}, holds); d != "" {
			t.Errorf("holds diff (-want,+got):\n%s", d)
		}
		if d := cmp.Diff([]element{t2}, th.FireAt(fireTime1), cmp.AllowUnexported(element{})); d != "" {
			t.Errorf("FireAt(%v) diff (-want,+got):\n%s", fireTime1, d)
		}
		if got := th.Peek(); got != mtime.MaxTimestamp {
			t.Errorf("Peek() after FireAt = %v, want %v", got, mtime.MaxTimestamp)
		}
	})

	t.Run("clearOneOfTwoDifferentFiring", func(t *testing.T) {
		// Clear the earlier timer and check that Peek advances to the later timer.
		th := newTimerHandler()
		holds := map[mtime.Time]int{}
		t1 := setElem(userKey1, eventTime1, holdTime1, "", glo)
		t2 := setElem(userKey2, eventTime2, holdTime2, "", glo)
		th.Persist(fireTime1, t1, holds)
		th.Persist(fireTime2, t2, holds)

		if mod := th.Persist(0, clearElem(userKey1, "", glo), holds); mod != -1 {
			t.Errorf("Persist(clear earlier) = %d, want -1", mod)
		}
		if got := th.Peek(); got != fireTime2 {
			t.Errorf("Peek() = %v, want %v", got, fireTime2)
		}
		if fired := th.FireAt(fireTime1); fired != nil {
			t.Errorf("FireAt(%v) = %v, want nil", fireTime1, fired)
		}
		if d := cmp.Diff([]element{t2}, th.FireAt(fireTime2), cmp.AllowUnexported(element{})); d != "" {
			t.Errorf("FireAt(%v) diff (-want,+got):\n%s", fireTime2, d)
		}

		// Clear the later timer and check that the earlier timer still fires.
		th = newTimerHandler()
		holds = map[mtime.Time]int{}
		th.Persist(fireTime1, t1, holds)
		th.Persist(fireTime2, t2, holds)

		if mod := th.Persist(0, clearElem(userKey2, "", glo), holds); mod != -1 {
			t.Errorf("Persist(clear later) = %d, want -1", mod)
		}
		if got := th.Peek(); got != fireTime1 {
			t.Errorf("Peek() = %v, want %v", got, fireTime1)
		}
		if d := cmp.Diff([]element{t1}, th.FireAt(fireTime2), cmp.AllowUnexported(element{})); d != "" {
			t.Errorf("FireAt(%v) diff (-want,+got):\n%s", fireTime2, d)
		}
		if got := th.Peek(); got != mtime.MaxTimestamp {
			t.Errorf("Peek() after FireAt = %v, want %v", got, mtime.MaxTimestamp)
		}
	})

	t.Run("setAgainAfterClear", func(t *testing.T) {
		th := newTimerHandler()
		holds := map[mtime.Time]int{}
		t1 := setElem(userKey1, eventTime1, holdTime1, "", glo)
		t2 := setElem(userKey1, eventTime2, holdTime2, "", glo)

		if mod := th.Persist(fireTime1, t1, holds); mod != 1 {
			t.Fatalf("Persist(first set) = %d, want 1", mod)
		}
		if mod := th.Persist(0, clearElem(userKey1, "", glo), holds); mod != -1 {
			t.Fatalf("Persist(clear) = %d, want -1", mod)
		}
		if mod := th.Persist(fireTime2, t2, holds); mod != 1 {
			t.Errorf("Persist(set after clear) = %d, want 1", mod)
		}
		if got := th.Peek(); got != fireTime2 {
			t.Errorf("Peek() = %v, want %v", got, fireTime2)
		}
		if d := cmp.Diff(map[mtime.Time]int{holdTime2: 1}, holds); d != "" {
			t.Errorf("holds diff (-want,+got):\n%s", d)
		}
		if fired := th.FireAt(fireTime1); fired != nil {
			t.Errorf("FireAt(%v) = %v, want nil", fireTime1, fired)
		}
		if d := cmp.Diff([]element{t2}, th.FireAt(fireTime2), cmp.AllowUnexported(element{})); d != "" {
			t.Errorf("FireAt(%v) diff (-want,+got):\n%s", fireTime2, d)
		}
		if got := th.Peek(); got != mtime.MaxTimestamp {
			t.Errorf("Peek() after FireAt = %v, want %v", got, mtime.MaxTimestamp)
		}

		// Clearing a prior bundle's hold and re-adding or replacing onto the same
		// hold timestamp in one holdChanges map cancels the delta back to empty.
		priorHolds := map[mtime.Time]int{}
		th.Persist(fireTime1, setElem(userKey1, eventTime1, holdTime1, "tag1", glo), priorHolds)
		th.Persist(fireTime1, setElem(userKey1, eventTime1, holdTime2, "tag2", glo), priorHolds)

		bundleHolds := map[mtime.Time]int{}
		th.Persist(0, clearElem(userKey1, "tag1", glo), bundleHolds)
		th.Persist(fireTime2, setElem(userKey1, eventTime2, holdTime1, "tag3", glo), bundleHolds)
		if d := cmp.Diff(map[mtime.Time]int{}, bundleHolds); d != "" {
			t.Errorf("bundleHolds after clear+insert diff (-want,+got):\n%s", d)
		}

		replaceHolds := map[mtime.Time]int{}
		th.Persist(0, clearElem(userKey1, "tag3", glo), replaceHolds)
		th.Persist(fireTime3, setElem(userKey1, eventTime2, holdTime1, "tag2", glo), replaceHolds)
		if d := cmp.Diff(map[mtime.Time]int{holdTime2: -1}, replaceHolds); d != "" {
			t.Errorf("replaceHolds after clear+replace diff (-want,+got):\n%s", d)
		}
	})
}
