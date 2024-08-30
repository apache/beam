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
	"container/heap"
	"context"
	"io"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/timers"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

type userTimerAdapter struct {
	sID           StreamID
	familyToSpec  map[string]timerFamilySpec
	modifications map[windowKeyPair]*timerModifications

	currentKey       any
	keyEncoded       bool
	buf              bytes.Buffer
	currentKeyString string
}

type timerFamilySpec struct {
	Domain     timers.TimeDomain
	KeyEncoder ElementEncoder
	KeyDecoder ElementDecoder
	WinEncoder WindowEncoder
	WinDecoder WindowDecoder
}

func newTimerFamilySpec(domain timers.TimeDomain, timerCoder *coder.Coder) timerFamilySpec {
	keyCoder := timerCoder.Components[0]
	return timerFamilySpec{
		Domain:     domain,
		KeyEncoder: MakeElementEncoder(keyCoder),
		KeyDecoder: MakeElementDecoder(keyCoder),
		WinEncoder: MakeWindowEncoder(timerCoder.Window),
		WinDecoder: MakeWindowDecoder(timerCoder.Window),
	}
}

// newUserTimerAdapter returns a user timer adapter for the given StreamID and timer coder.
func newUserTimerAdapter(sID StreamID, familyToSpec map[string]timerFamilySpec) *userTimerAdapter {
	return &userTimerAdapter{sID: sID, familyToSpec: familyToSpec}
}

// SetCurrentKey keeps the key around so we can encoded if needed for timers.
func (u *userTimerAdapter) SetCurrentKey(mainIn *MainInput) {
	if u == nil {
		return
	}
	u.currentKey = mainIn.Key.Elm
	u.keyEncoded = false
}

// SetCurrentKeyString is for processing timer callbacks, and avoids re-encoding the key.
func (u *userTimerAdapter) SetCurrentKeyString(key string) {
	if u == nil {
		return
	}
	u.currentKeyString = key
	u.keyEncoded = true
}

// GetKeyString encodes the current key with the family's encoder, and stores the string
// for later access.
func (u *userTimerAdapter) GetKeyStringAndDomain(family string) (string, timers.TimeDomain) {
	if u.keyEncoded {
		return u.currentKeyString, u.familyToSpec[family].Domain
	}
	spec := u.familyToSpec[family]

	u.buf.Reset()
	if err := spec.KeyEncoder.Encode(&FullValue{Elm: u.currentKey}, &u.buf); err != nil {
		panic(err)
	}
	u.currentKeyString = u.buf.String()
	u.keyEncoded = true
	return u.currentKeyString, spec.Domain
}

// NewTimerProvider creates and returns a timer provider to set/clear timers.
func (u *userTimerAdapter) NewTimerProvider(pane typex.PaneInfo, w []typex.Window) *timerProvider {
	return &timerProvider{
		window:  w,
		pane:    pane,
		adapter: u,
	}
}

func (u *userTimerAdapter) GetModifications(key windowKeyPair) *timerModifications {
	if u.modifications == nil {
		u.modifications = map[windowKeyPair]*timerModifications{}
	}
	mods, ok := u.modifications[key]
	if !ok {
		mods = &timerModifications{
			modified: map[timerKey]sortableTimer{},
		}
		u.modifications[key] = mods
	}
	return mods
}

// FlushAndReset writes all outstanding modified timers to the datamanager.
func (u *userTimerAdapter) FlushAndReset(ctx context.Context, manager DataManager) error {
	if u == nil {
		return nil
	}
	writersByFamily := map[string]io.Writer{}

	var b bytes.Buffer
	for windowKeyPair, mods := range u.modifications {
		for id, timer := range mods.modified {
			spec := u.familyToSpec[id.family]
			w, ok := writersByFamily[id.family]
			if !ok {
				var err error
				w, err = manager.OpenTimerWrite(ctx, u.sID, id.family)
				if err != nil {
					return err
				}
				writersByFamily[id.family] = w
			}
			b.Reset()
			b.Write([]byte(windowKeyPair.key))

			if err := encodeTimerSuffix(spec.WinEncoder, TimerRecv{
				TimerMap: timer.TimerMap,
				Windows:  []typex.Window{windowKeyPair.window},
				Pane:     timer.Pane,
			}, &b); err != nil {
				return errors.WithContextf(err, "error writing timer family %v, tag %v", timer.Family, timer.Tag)
			}
			w.Write(b.Bytes())
		}
	}

	u.modifications = nil
	u.currentKey = nil
	u.currentKeyString = ""
	u.keyEncoded = false
	return nil
}

type timerProvider struct {
	window []typex.Window
	pane   typex.PaneInfo

	adapter *userTimerAdapter
}

// Set writes a new timer. This can be used to both Set as well as Clear the timer.
// Note: This function is intended for internal use only.
func (p *timerProvider) Set(t timers.TimerMap) {
	k, domain := p.adapter.GetKeyStringAndDomain(t.Family)
	for _, w := range p.window {
		modifications := p.adapter.GetModifications(windowKeyPair{w, k})

		// Make the adapter know the domains of the families.
		insertedTimer := sortableTimer{
			Domain:   domain,
			TimerMap: t,
			Pane:     p.pane,
		}
		modifications.InsertTimer(insertedTimer)
	}
}

// TimerRecv holds the timer metadata while encoding and decoding timers in exec unit.
//
// For SDK internal use, and subject to change.
type TimerRecv struct {
	Key             *FullValue
	KeyString       string // The bytes for the key to avoid re-encoding key for lookups.
	Windows         []typex.Window
	Pane            typex.PaneInfo
	timers.TimerMap // embed common information from set parameter.
}

// timerHeap is a min heap for inserting user set timers, and allowing for inline
// processing of timers in the bundle.
type timerHeap []sortableTimer

type sortableTimer struct {
	Domain timers.TimeDomain

	timers.TimerMap

	Pane typex.PaneInfo
}

// Cleared returns the same timer, but in cleared form.
func (st sortableTimer) Cleared() sortableTimer {
	// Must be a non-pointer receiver, so this returns a copy.
	st.Clear = true
	st.FireTimestamp = 0
	st.HoldTimestamp = 0
	st.Pane = typex.PaneInfo{}
	return st
}

func (st sortableTimer) Less(right sortableTimer) bool {
	left := st
	// Sort Processing time timers before event time timers, as they tend to be more latency sensitive.
	// There's also the "unspecified" timer, which is treated like an Event Time at present.
	if left.Domain != right.Domain {
		return left.Domain == timers.ProcessingTimeDomain && right.Domain != timers.ProcessingTimeDomain
	}

	// Sort cleared timers first, so newly written fire-able timers can fire.
	if left.Clear != right.Clear {
		return left.Clear && !right.Clear
	}

	if left.FireTimestamp != right.FireTimestamp {
		return left.FireTimestamp < right.FireTimestamp
	}
	if left.HoldTimestamp != right.HoldTimestamp {
		return left.HoldTimestamp < right.HoldTimestamp
	}

	return left.Tag < right.Tag
}

var _ heap.Interface = (*timerHeap)(nil)

// Len satisfies the sort interface invariant.
func (h timerHeap) Len() int { return len(h) }

// Less satisfies the sort interface invariant.
func (h timerHeap) Less(i, j int) bool {
	left, right := h[i], h[j]
	return left.Less(right)
}

// Swap satisfies the sort interface invariant.
// Intended for use only by the timerHeap itself.
func (h timerHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push satisfies the heap interface invariant.
// Intended for use only by the timerHeap itself.
func (h *timerHeap) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(sortableTimer))
}

// Pop satisfies the heap interface invariant.
// Intended for use only by the timerHeap itself.
func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Add timers to the heap.
func (h *timerHeap) Add(timer sortableTimer) {
	heap.Push(h, timer)
}

// HeadSetIter gets an iterator for all timers sorted less than or equal to the given timer.
// The iterator function is a view over this timerheap, so changes to this heap will be
// reflected in the iterator.
//
// The iterator is not safe to be used on multiple goroutines.
func (h *timerHeap) HeadSetIter(timer sortableTimer) func() (sortableTimer, bool) {
	return func() (sortableTimer, bool) {
		if h.Len() > 0 && ((*h)[0].Less(timer) || (*h)[0] == timer) {
			return heap.Pop(h).(sortableTimer), true
		}
		return sortableTimer{}, false
	}
}

type timerKey struct {
	family, tag string
}

type windowKeyPair struct {
	window typex.Window
	key    string
}

type timerModifications struct {
	// Track timer modifications per domain.
	earlierTimers [3]timerHeap
	// TIMER FAMILY + TAG, have the actual updated timers.
	modified map[timerKey]sortableTimer
}

func (tm *timerModifications) InsertTimer(t sortableTimer) {
	tm.modified[timerKey{
		family: t.Family,
		tag:    t.Tag,
	}] = t
	tm.earlierTimers[t.Domain].Add(t)
}

func (tm *timerModifications) IsModified(check sortableTimer) bool {
	got, ok := tm.modified[timerKey{
		family: check.Family,
		tag:    check.Tag,
	}]
	if !ok {
		return false
	}
	return got != check
}
