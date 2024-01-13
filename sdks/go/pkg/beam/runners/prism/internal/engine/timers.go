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
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"google.golang.org/protobuf/encoding/protowire"
)

// DecodeTimer extracts timers to elements for insertion into their keyed queues.
// Returns the key bytes, tag, window exploded elements, and the hold timestamp.
// If the timer has been cleared, no elements will be returned. Any existing timers
// for the tag *must* be cleared from the pending queue.
func decodeTimer(keyDec func(io.Reader) []byte, usesGlobalWindow bool, raw []byte) ([]byte, string, []element) {
	keyBytes := keyDec(bytes.NewBuffer(raw))

	d := decoder{raw: raw, cursor: len(keyBytes)}
	tag := string(d.Bytes())

	var ws []typex.Window
	numWin := d.Fixed32()
	if usesGlobalWindow {
		for i := 0; i < int(numWin); i++ {
			ws = append(ws, window.GlobalWindow{})
		}
	} else {
		// Assume interval windows here, since we don't understand custom windows yet.
		for i := 0; i < int(numWin); i++ {
			ws = append(ws, d.IntervalWindow())
		}
	}

	clear := d.Bool()
	hold := mtime.MaxTimestamp
	if clear {
		return keyBytes, tag, nil
	}

	firing := d.Timestamp()
	hold = d.Timestamp()
	pane := d.Pane()

	var ret []element
	for _, w := range ws {
		ret = append(ret, element{
			tag:           tag,
			elmBytes:      nil, // indicates this is a timer.
			keyBytes:      keyBytes,
			window:        w,
			timestamp:     firing,
			holdTimestamp: hold,
			pane:          pane,
		})
	}
	return keyBytes, tag, ret
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
