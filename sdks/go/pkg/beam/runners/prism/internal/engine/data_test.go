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
	"math"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestCompareTimestampSuffixes(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		loI := int64(math.MinInt64)
		hiI := int64(math.MaxInt64)

		loB := binary.BigEndian.AppendUint64(nil, uint64(loI))
		hiB := binary.BigEndian.AppendUint64(nil, uint64(hiI))

		if compareTimestampSuffixes(loB, hiB) != (loI < hiI) {
			t.Errorf("lo vs Hi%v < %v: bytes %v vs %v, %v %v", loI, hiI, loB, hiB, loI < hiI, compareTimestampSuffixes(loB, hiB))
		}
	})
}

func TestOrderedListState(t *testing.T) {
	time1 := protowire.AppendVarint(nil, 11)
	time2 := protowire.AppendVarint(nil, 22)
	time3 := protowire.AppendVarint(nil, 33)
	time4 := protowire.AppendVarint(nil, 44)
	time5 := protowire.AppendVarint(nil, 55)

	wKey := []byte{} // global window.
	uKey := []byte("\u0007userkey")
	linkID := LinkID{
		Transform: "dofn",
		Local:     "localStateName",
	}
	cc := func(a []byte, b ...byte) []byte {
		return bytes.Join([][]byte{a, b}, []byte{})
	}

	t.Run("bool", func(t *testing.T) {
		d := TentativeData{
			stateTypeLen: map[LinkID]valueLen{
				linkID: oneByteLen,
			},
		}

		d.AppendOrderedListState(linkID, wKey, uKey, cc(time3, 1))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time2, 0))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time5, 1))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time1, 1))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time4, 0))

		got := d.GetOrderedListState(linkID, wKey, uKey, 0, 60)
		want := [][]byte{
			cc(time1, 1),
			cc(time2, 0),
			cc(time3, 1),
			cc(time4, 0),
			cc(time5, 1),
		}
		if d := cmp.Diff(want, got); d != "" {
			t.Errorf("OrderedList booleans \n%v", d)
		}

		d.ClearOrderedListState(linkID, wKey, uKey, 12, 54)
		got = d.GetOrderedListState(linkID, wKey, uKey, 0, 60)
		want = [][]byte{
			cc(time1, 1),
			cc(time5, 1),
		}
		if d := cmp.Diff(want, got); d != "" {
			t.Errorf("OrderedList booleans, after clear\n%v", d)
		}
	})
	t.Run("int32", func(t *testing.T) {
		d := TentativeData{
			stateTypeLen: map[LinkID]valueLen{
				linkID: fourByteLen,
			},
		}

		d.AppendOrderedListState(linkID, wKey, uKey, cc(time5, 0, 0, 0, 1))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time4, 0, 0, 1, 0))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time3, 0, 1, 0, 0))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time2, 1, 0, 0, 0))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time1, 1, 0, 0, 1))

		got := d.GetOrderedListState(linkID, wKey, uKey, 0, 60)
		want := [][]byte{
			cc(time1, 1, 0, 0, 1),
			cc(time2, 1, 0, 0, 0),
			cc(time3, 0, 1, 0, 0),
			cc(time4, 0, 0, 1, 0),
			cc(time5, 0, 0, 0, 1),
		}
		if d := cmp.Diff(want, got); d != "" {
			t.Errorf("OrderedList int32 \n%v", d)
		}
	})
	t.Run("float64", func(t *testing.T) {
		d := TentativeData{
			stateTypeLen: map[LinkID]valueLen{
				linkID: eightByteLen,
			},
		}

		d.AppendOrderedListState(linkID, wKey, uKey, cc(time5, 0, 0, 0, 0, 0, 0, 0, 1))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time1, 0, 0, 0, 0, 0, 0, 1, 0))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time3, 0, 0, 0, 0, 0, 1, 0, 0))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time2, 0, 0, 0, 0, 1, 0, 0, 0))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time4, 0, 0, 0, 1, 0, 0, 0, 0))

		got := d.GetOrderedListState(linkID, wKey, uKey, 0, 60)
		want := [][]byte{
			cc(time1, 0, 0, 0, 0, 0, 0, 1, 0),
			cc(time2, 0, 0, 0, 0, 1, 0, 0, 0),
			cc(time3, 0, 0, 0, 0, 0, 1, 0, 0),
			cc(time4, 0, 0, 0, 1, 0, 0, 0, 0),
			cc(time5, 0, 0, 0, 0, 0, 0, 0, 1),
		}
		if d := cmp.Diff(want, got); d != "" {
			t.Errorf("OrderedList float64s \n%v", d)
		}

		d.ClearOrderedListState(linkID, wKey, uKey, 11, 12)
		d.ClearOrderedListState(linkID, wKey, uKey, 33, 34)
		d.ClearOrderedListState(linkID, wKey, uKey, 55, 56)

		got = d.GetOrderedListState(linkID, wKey, uKey, 0, 60)
		want = [][]byte{
			cc(time2, 0, 0, 0, 0, 1, 0, 0, 0),
			cc(time4, 0, 0, 0, 1, 0, 0, 0, 0),
		}
		if d := cmp.Diff(want, got); d != "" {
			t.Errorf("OrderedList float64s, after clear \n%v", d)
		}
	})

	t.Run("varint", func(t *testing.T) {
		d := TentativeData{
			stateTypeLen: map[LinkID]valueLen{
				linkID: varIntLen,
			},
		}

		d.AppendOrderedListState(linkID, wKey, uKey, cc(time2, protowire.AppendVarint(nil, 56)...))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time4, protowire.AppendVarint(nil, 20067)...))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time3, protowire.AppendVarint(nil, 7777777)...))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time1, protowire.AppendVarint(nil, 424242)...))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time5, protowire.AppendVarint(nil, 0)...))

		got := d.GetOrderedListState(linkID, wKey, uKey, 0, 60)
		want := [][]byte{
			cc(time1, protowire.AppendVarint(nil, 424242)...),
			cc(time2, protowire.AppendVarint(nil, 56)...),
			cc(time3, protowire.AppendVarint(nil, 7777777)...),
			cc(time4, protowire.AppendVarint(nil, 20067)...),
			cc(time5, protowire.AppendVarint(nil, 0)...),
		}
		if d := cmp.Diff(want, got); d != "" {
			t.Errorf("OrderedList int32 \n%v", d)
		}
	})
	t.Run("lp", func(t *testing.T) {
		d := TentativeData{
			stateTypeLen: map[LinkID]valueLen{
				linkID: lenPrefiedLen,
			},
		}

		d.AppendOrderedListState(linkID, wKey, uKey, cc(time1, []byte("\u0003one")...))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time2, []byte("\u0003two")...))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time3, []byte("\u0005three")...))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time4, []byte("\u0004four")...))
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time5, []byte("\u0019FourHundredAndEleventyTwo")...))

		got := d.GetOrderedListState(linkID, wKey, uKey, 0, 60)
		want := [][]byte{
			cc(time1, []byte("\u0003one")...),
			cc(time2, []byte("\u0003two")...),
			cc(time3, []byte("\u0005three")...),
			cc(time4, []byte("\u0004four")...),
			cc(time5, []byte("\u0019FourHundredAndEleventyTwo")...),
		}
		if d := cmp.Diff(want, got); d != "" {
			t.Errorf("OrderedList int32 \n%v", d)
		}
	})
	t.Run("lp_onecall", func(t *testing.T) {
		d := TentativeData{
			stateTypeLen: map[LinkID]valueLen{
				linkID: lenPrefiedLen,
			},
		}
		d.AppendOrderedListState(linkID, wKey, uKey, bytes.Join([][]byte{
			time5, []byte("\u0019FourHundredAndEleventyTwo"),
			time3, []byte("\u0005three"),
			time2, []byte("\u0003two"),
			time1, []byte("\u0003one"),
			time4, []byte("\u0004four"),
		}, nil))

		got := d.GetOrderedListState(linkID, wKey, uKey, 0, 60)
		want := [][]byte{
			cc(time1, []byte("\u0003one")...),
			cc(time2, []byte("\u0003two")...),
			cc(time3, []byte("\u0005three")...),
			cc(time4, []byte("\u0004four")...),
			cc(time5, []byte("\u0019FourHundredAndEleventyTwo")...),
		}
		if d := cmp.Diff(want, got); d != "" {
			t.Errorf("OrderedList int32 \n%v", d)
		}
	})
}
