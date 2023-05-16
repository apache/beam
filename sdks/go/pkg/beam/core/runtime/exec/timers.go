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
	"container/heap"
	"context"
	"fmt"
	"io"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/timers"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

// UserTimerAdapter provides a timer provider to be used for manipulating timers.
type UserTimerAdapter interface {
	NewTimerProvider(ctx context.Context, manager DataManager, inputTimestamp typex.EventTime, windows []typex.Window, element *MainInput) (timerProvider, error)
}

type userTimerAdapter struct {
	sID StreamID
	ec  ElementEncoder
	dc  ElementDecoder
	wc  WindowDecoder
}

// NewUserTimerAdapter returns a user timer adapter for the given StreamID and timer coder.
func NewUserTimerAdapter(sID StreamID, c *coder.Coder, timerCoder *coder.Coder) UserTimerAdapter {
	if !coder.IsW(c) {
		panic(fmt.Sprintf("expected WV coder for user timer %v: %v", sID, c))
	}
	ec := MakeElementEncoder(timerCoder)
	dc := MakeElementDecoder(coder.SkipW(c).Components[0])
	wc := MakeWindowDecoder(c.Window)
	return &userTimerAdapter{sID: sID, ec: ec, wc: wc, dc: dc}
}

// NewTimerProvider creates and returns a timer provider to set/clear timers.
func (u *userTimerAdapter) NewTimerProvider(ctx context.Context, manager DataManager, inputTs typex.EventTime, w []typex.Window, element *MainInput) (timerProvider, error) {
	userKey := &FullValue{Elm: element.Key.Elm}
	tp := timerProvider{
		ctx:                 ctx,
		tm:                  manager,
		userKey:             userKey,
		inputTimestamp:      inputTs,
		sID:                 u.sID,
		window:              w,
		writersByFamily:     make(map[string]io.Writer),
		timerElementEncoder: u.ec,
		keyElementDecoder:   u.dc,
	}

	return tp, nil
}

type timerProvider struct {
	ctx            context.Context
	tm             DataManager
	sID            StreamID
	inputTimestamp typex.EventTime
	userKey        *FullValue
	window         []typex.Window

	pn typex.PaneInfo

	writersByFamily     map[string]io.Writer
	timerElementEncoder ElementEncoder
	keyElementDecoder   ElementDecoder
}

func (p *timerProvider) getWriter(family string) (io.Writer, error) {
	if w, ok := p.writersByFamily[family]; ok {
		return w, nil
	}
	w, err := p.tm.OpenTimerWrite(p.ctx, p.sID, family)
	if err != nil {
		return nil, err
	}
	p.writersByFamily[family] = w
	return p.writersByFamily[family], nil
}

// Set writes a new timer. This can be used to both Set as well as Clear the timer.
// Note: This function is intended for internal use only.
func (p *timerProvider) Set(t timers.TimerMap) {
	w, err := p.getWriter(t.Family)
	if err != nil {
		panic(err)
	}
	tm := TimerRecv{
		Key:           p.userKey,
		Tag:           t.Tag,
		Windows:       p.window,
		Clear:         t.Clear,
		FireTimestamp: t.FireTimestamp,
		HoldTimestamp: t.HoldTimestamp,
		Pane:          p.pn,
	}
	fv := FullValue{Elm: tm}
	if err := p.timerElementEncoder.Encode(&fv, w); err != nil {
		panic(err)
	}
}

// TimerRecv holds the timer metadata while encoding and decoding timers in exec unit.
type TimerRecv struct {
	Key                          *FullValue
	Tag                          string
	Windows                      []typex.Window // []typex.Window
	Clear                        bool
	FireTimestamp, HoldTimestamp mtime.Time
	Pane                         typex.PaneInfo
}

// timerHeap is a min heap for inserting user set timers, and allowing for inline
// processing of timers in the bundle.
type timerHeap []sortableTimer

type sortableTimer struct {
	Domain timers.TimeDomain
	timers.TimerMap
}

func (left sortableTimer) Less(right sortableTimer) bool {
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

// HeadSet gets all timers sorted less than or equal to the given timer.
func (h *timerHeap) HeadSet(timer sortableTimer) []sortableTimer {
	if h.Len() == 0 {
		return nil
	}
	var ret []sortableTimer
	for h.Len() > 0 && (*h)[0].Less(timer) {
		ret = append(ret, heap.Pop(h).(sortableTimer))
	}
	if h.Len() > 0 && (*h)[0] == timer {
		ret = append(ret, heap.Pop(h).(sortableTimer))
	}
	return ret
}
