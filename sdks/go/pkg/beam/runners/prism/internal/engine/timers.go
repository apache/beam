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
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"google.golang.org/protobuf/encoding/protowire"
)

type timerRet struct {
	keyBytes []byte
	tag      string
	elms     []element
	windows  []typex.Window
}

// decodeTimerIter extracts timers to elements for insertion into their keyed queues,
// through a go iterator function, to be called by the caller with their processing function.
//
// For each timer, a key, tag, windowed elements, and the window set are returned.
//
// If the timer has been cleared, no elements will be returned. Any existing timers
// for the tag *must* be cleared from the pending queue. The windows associated with
// the clear are provided to be able to delete pending timers.
func decodeTimerIter(keyDec func(io.Reader) []byte, winCoder WinCoderType, raw []byte) func(func(timerRet) bool) {

	var singleWindowExtractor func(*decoder) typex.Window
	switch winCoder {
	case WinGlobal:
		singleWindowExtractor = func(*decoder) typex.Window {
			return window.GlobalWindow{}
		}
	case WinInterval:
		singleWindowExtractor = func(d *decoder) typex.Window {
			return d.IntervalWindow()
		}
	case WinCustom:
		// Default to a length prefixed window coder here until we have different information.
		// Everything else is either:: variable, 1,  4, or 8 bytes long
		// KVs (especially nested ones, could occur but are unlikely, and it would be
		// easier for Prism to force such coders to be length prefixed.
		singleWindowExtractor = func(d *decoder) typex.Window {
			return d.CustomWindowLengthPrefixed()
		}
	default:
		// Unsupported
		panic(fmt.Sprintf("unsupported WindowCoder Type: %v", winCoder))
	}

	return func(yield func(timerRet) bool) {
		for len(raw) > 0 {
			keyBytes := keyDec(bytes.NewBuffer(raw))
			d := decoder{raw: raw, cursor: len(keyBytes)}
			tag := string(d.Bytes())

			var ws []typex.Window
			numWin := d.Fixed32()
			for i := 0; i < int(numWin); i++ {
				ws = append(ws, singleWindowExtractor(&d))
			}

			clear := d.Bool()
			hold := mtime.MaxTimestamp
			if clear {
				if !yield(timerRet{keyBytes, tag, nil, ws}) {
					return // Halt iteration if yeild returns false.
				}
				// Otherwise continue handling the remaining bytes.
				raw = d.UnusedBytes()
				continue
			}

			firing := d.Timestamp()
			hold = d.Timestamp()
			pane := d.Pane()

			var elms []element
			for _, w := range ws {
				elms = append(elms, element{
					tag:           tag,
					elmBytes:      nil, // indicates this is a timer.
					keyBytes:      keyBytes,
					window:        w,
					timestamp:     firing,
					holdTimestamp: hold,
					pane:          pane,
					sequence:      len(elms),
				})
			}

			if !yield(timerRet{keyBytes, tag, elms, ws}) {
				return // Halt iteration if yeild returns false.
			}
			// Otherwise continue handling the remaining bytes.
			raw = d.UnusedBytes()
		}
	}
}

type decoder struct {
	raw    []byte
	cursor int
}

// Varint consumes a varint from the bytes, returning the decoded length.
func (d *decoder) Varint() (l int64) {
	v, n := protowire.ConsumeVarint(d.raw[d.cursor:])
	if n < 0 {
		panic("invalid varint")
	}
	d.cursor += n
	return int64(v)
}

// Uint64 decodes a value of type uint64.
func (d *decoder) Uint64() uint64 {
	defer func() {
		d.cursor += 8
	}()
	return binary.BigEndian.Uint64(d.raw[d.cursor : d.cursor+8])
}

func (d *decoder) Timestamp() mtime.Time {
	msec := d.Uint64()
	return mtime.Time((int64)(msec) + math.MinInt64)
}

// Fixed32 decodes a fixed length encoding of uint32, for window decoding.
func (d *decoder) Fixed32() uint32 {
	defer func() {
		d.cursor += 4
	}()
	return binary.BigEndian.Uint32(d.raw[d.cursor : d.cursor+4])
}

func (d *decoder) IntervalWindow() window.IntervalWindow {
	end := d.Timestamp()
	dur := d.Varint()
	return window.IntervalWindow{
		End:   end,
		Start: mtime.FromMilliseconds(end.Milliseconds() - dur),
	}
}

// CustomWindowLengthPrefixed assumes the custom window coder is a variable, length prefixed type
// such as string, bytes, or a length prefix wrapped coder.
func (d *decoder) CustomWindowLengthPrefixed() customWindow {
	end := d.Timestamp()

	customStart := d.cursor
	l := d.Varint()
	endCursor := d.cursor + int(l)
	d.cursor = endCursor
	return customWindow{
		End:    end,
		Custom: d.raw[customStart:endCursor],
	}
}

type customWindow struct {
	End    typex.EventTime
	Custom []byte // The custom portion of the window, ignored by the runner
}

func (w customWindow) MaxTimestamp() typex.EventTime {
	return w.End
}

func (w customWindow) Equals(o typex.Window) bool {
	if c, ok := o.(customWindow); ok {
		return w.End == c.End && bytes.Equal(w.Custom, c.Custom)
	}
	return false
}

func (d *decoder) Byte() byte {
	defer func() {
		d.cursor += 1
	}()
	return d.raw[d.cursor]
}

func (d *decoder) Bytes() []byte {
	l := d.Varint()
	end := d.cursor + int(l)
	b := d.raw[d.cursor:end]
	d.cursor = end
	return b
}

// UnusedBytes returns the remainder of bytes in the buffer that weren't yet used.
// Multiple timers can be provided in a single timers buffer, since multiple dynamic
// timer tags may be set.
func (d *decoder) UnusedBytes() []byte {
	return d.raw[d.cursor:]
}

func (d *decoder) Bool() bool {
	if b := d.Byte(); b == 0 {
		return false
	} else if b == 1 {
		return true
	} else {
		panic(fmt.Sprintf("unable to decode bool; expected {0, 1} got %v", b))
	}
}

func (d *decoder) Pane() typex.PaneInfo {
	first := d.Byte()
	pn := coder.NewPane(first & 0x0f)

	switch first >> 4 {
	case 0:
		// Result encoded in only one pane.
		return pn
	case 1:
		// Result encoded in one pane plus a VarInt encoded integer.
		index := d.Varint()
		pn.Index = index
		if pn.Timing == typex.PaneEarly {
			pn.NonSpeculativeIndex = -1
		} else {
			pn.NonSpeculativeIndex = pn.Index
		}
	case 2:
		// Result encoded in one pane plus two VarInt encoded integer.
		index := d.Varint()
		pn.Index = index
		pn.NonSpeculativeIndex = d.Varint()
	}
	return pn
}

// timerHandler tracks timers and ensures that the timer invariant is maintained
// and reports changes in watermark holds.
//
// The invariant is that only a single timer exists for a given userKey+timerID+tag+window.
//
// Timers may prevent the event time watermark using a watermark Hold.
// However due to the invariant, the watermark hold must be changed if a given timer
// has it's firing + hold time updated.
//
// A timerHandler may only hold timers of a single domain, either event time timers, or
// processing time timers. They must not be mixed.
type timerHandler struct {
	order      mtimeHeap                               // Maintain the next times to fire at.
	toFire     map[mtime.Time]map[string]set[timerKey] // fireing time -> userkey -> timerID+tag+window: Lookup
	nextFiring map[string]map[timerKey]fireElement     // userkey -> timerID+tag+window: actual timer

	timerKeySetPool sync.Pool // set[timerKey]{}
	userKeysSetPool sync.Pool // map[string]set[timerKey]
	firingMapPool   sync.Pool // map[timerKey]element
}

type fireElement struct {
	firing mtime.Time
	timer  element
}

func newTimerHandler() *timerHandler {
	return &timerHandler{
		toFire:     map[mtime.Time]map[string]set[timerKey]{},
		nextFiring: map[string]map[timerKey]fireElement{},

		timerKeySetPool: sync.Pool{New: func() any {
			return set[timerKey]{}
		}},
		userKeysSetPool: sync.Pool{New: func() any {
			return map[string]set[timerKey]{}
		}},
		firingMapPool: sync.Pool{New: func() any {
			return map[timerKey]fireElement{}
		}},
	}
}

// timers returns the timers for the userkey.
func (th *timerHandler) timers(timer element) map[timerKey]fireElement {
	timers, ok := th.nextFiring[string(timer.keyBytes)]
	if !ok {
		timers = th.firingMapPool.Get().(map[timerKey]fireElement)
		th.nextFiring[string(timer.keyBytes)] = timers
	}
	return timers
}

func (th *timerHandler) removeTimer(userKey string, key timerKey) element {
	timers, ok := th.nextFiring[userKey]
	if !ok {
		panic(fmt.Sprintf("prism consistency error: trying to remove a timer for a key without timers: %v,%+v", userKey, key))
	}
	times, ok := timers[key]
	if !ok {
		panic(fmt.Sprintf("prism consistency error: trying to remove a non-existent timer for a key: %v,%+v", userKey, key))
	}
	delete(timers, key)
	if len(timers) == 0 {
		delete(th.nextFiring, userKey)
		th.firingMapPool.Put(timers)
	}
	return times.timer
}

func (th *timerHandler) add(key timerKey, newFire fireElement) {
	byKeys, ok := th.toFire[newFire.firing]
	if !ok {
		byKeys = th.userKeysSetPool.Get().(map[string]set[timerKey])
		th.toFire[newFire.firing] = byKeys
		heap.Push(&th.order, newFire.firing) // We only need to add a firing order when inserting.
	}
	timers, ok := byKeys[string(newFire.timer.keyBytes)]
	if !ok {
		timers = th.timerKeySetPool.Get().(set[timerKey])
		byKeys[string(newFire.timer.keyBytes)] = timers

	}
	timers.insert(key)

}

func (th *timerHandler) replace(key timerKey, oldTimer, newTimer fireElement) {
	byKeys := th.toFire[oldTimer.firing]
	timers := byKeys[string(oldTimer.timer.keyBytes)]
	timers.remove(key)

	th.add(key, newTimer)

	// Clean up timers.
	if len(timers) == 0 {
		th.timerKeySetPool.Put(timers)
		delete(byKeys, string(oldTimer.timer.keyBytes))
	}
	if len(byKeys) > 0 {
		return
	}
	th.userKeysSetPool.Put(byKeys)
	delete(th.toFire, oldTimer.firing)
	th.order.Remove(oldTimer.firing)
}

// Persist the given timer, and updates the provided hold times map with changes to the hold counts.
func (th *timerHandler) Persist(fire mtime.Time, timer element, holdChanges map[mtime.Time]int) int {
	timers := th.timers(timer)
	key := timerKey{family: timer.family, tag: timer.tag, window: timer.window}
	newTimer := fireElement{firing: fire, timer: timer}
	if oldTimer, ok := timers[key]; ok {
		// Update with the new times
		timers[key] = newTimer
		th.replace(key, oldTimer, newTimer)

		holdChanges[newTimer.timer.holdTimestamp] += 1
		holdChanges[oldTimer.timer.holdTimestamp] -= 1
		if holdChanges[oldTimer.timer.holdTimestamp] == 0 {
			delete(holdChanges, oldTimer.timer.holdTimestamp)
		}
		return 0
	}
	timers[key] = newTimer
	th.add(key, newTimer)
	holdChanges[newTimer.timer.holdTimestamp] += 1
	return 1
}

// FireAt returns all timers for a key able to fire at the given time.
func (th *timerHandler) FireAt(now mtime.Time) []element {
	if th.order.Len() == 0 {
		return nil
	}
	var ret []element
	for len(th.order) > 0 && th.order[0] <= now {
		next := th.order[0]
		byKeys, ok := th.toFire[next]
		if ok {
			for k, vs := range byKeys {
				for v := range vs {
					timer := th.removeTimer(k, v)
					ret = append(ret, timer)
				}
				delete(byKeys, k)
			}
		}
		delete(th.toFire, next)
		th.userKeysSetPool.Put(byKeys)
		heap.Pop(&th.order)
	}
	return ret
}

// Peek returns the next scheduled event in the queue.
// Returns [mtime.MaxTimestamp] if the queue is empty.
func (th *timerHandler) Peek() mtime.Time {
	if th.order.Len() == 0 {
		return mtime.MaxTimestamp
	}
	return th.order[0]
}
