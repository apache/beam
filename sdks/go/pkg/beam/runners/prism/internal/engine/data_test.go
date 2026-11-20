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
	"slices"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
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
			stateTypeLen: map[LinkID]func([]byte) int{
				linkID: func(_ []byte) int {
					return 1
				},
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
	t.Run("float64", func(t *testing.T) {
		d := TentativeData{
			stateTypeLen: map[LinkID]func([]byte) int{
				linkID: func(_ []byte) int {
					return 8
				},
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
			stateTypeLen: map[LinkID]func([]byte) int{
				linkID: func(b []byte) int {
					_, n := protowire.ConsumeVarint(b)
					return int(n)
				},
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
			stateTypeLen: map[LinkID]func([]byte) int{
				linkID: func(b []byte) int {
					l, n := protowire.ConsumeVarint(b)
					return int(l) + n
				},
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
			stateTypeLen: map[LinkID]func([]byte) int{
				linkID: func(b []byte) int {
					l, n := protowire.ConsumeVarint(b)
					return int(l) + n
				},
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
	t.Run("clear_uninitialized", func(t *testing.T) {
		d := TentativeData{
			stateTypeLen: map[LinkID]func([]byte) int{
				linkID: func(_ []byte) int {
					return 1
				},
			},
		}

		// d.state is nil.
		d.ClearOrderedListState(linkID, wKey, uKey, math.MinInt64, math.MaxInt64)
		if d.state != nil {
			t.Fatalf("ClearOrderedListState on empty TentativeData allocated state map: %v", d.state)
		}

		// d.state is non-nil, but has no entry for linkID.
		otherLink := LinkID{Transform: "dofn", Local: "otherState"}
		d.AppendBagState(otherLink, wKey, uKey, []byte{1})
		d.ClearOrderedListState(linkID, wKey, uKey, 0, 100)
		if _, ok := d.state[linkID]; ok {
			t.Fatalf("ClearOrderedListState created entry for uninitialized stateID %v", linkID)
		}

		// d.state[linkID] exists for an interval window, but not for the global window.
		// An SDK that flushes range clears before appends on the first bundle in a window
		// must not panic and must preserve subsequent appends.
		iwKey := make([]byte, 16)
		binary.BigEndian.PutUint64(iwKey[0:8], uint64(1000)^0x8000000000000000)
		d.AppendOrderedListState(linkID, iwKey, uKey, cc(time1, 1))
		d.ClearOrderedListState(linkID, wKey, uKey, 0, 30)
		d.AppendOrderedListState(linkID, wKey, uKey, cc(time2, 1))
		got := d.GetOrderedListState(linkID, wKey, uKey, 0, 60)
		want := [][]byte{cc(time2, 1)}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("OrderedList after clear_uninitialized + append:\n%v", diff)
		}
	})
}

func TestMultimapState(t *testing.T) {
	wKey := []byte{}
	uKey := []byte("userkey")
	linkID := LinkID{
		Transform: "dofn",
		Local:     "mm",
	}

	sortedKeys := func(d *TentativeData) [][]byte {
		ks := d.GetMultimapKeysState(linkID, wKey, uKey)
		slices.SortFunc(ks, bytes.Compare)
		return ks
	}

	t.Run("append_and_clear_one_key", func(t *testing.T) {
		d := TentativeData{}
		d.AppendMultimapState(linkID, wKey, uKey, []byte("a"), []byte("1"))
		d.AppendMultimapState(linkID, wKey, uKey, []byte("a"), []byte("2"))
		d.AppendMultimapState(linkID, wKey, uKey, []byte("b"), []byte("3"))

		if diff := cmp.Diff([][]byte{[]byte("1"), []byte("2")}, d.GetMultimapState(linkID, wKey, uKey, []byte("a"))); diff != "" {
			t.Errorf("GetMultimapState(a) (-want, +got):\n%v", diff)
		}
		if diff := cmp.Diff([][]byte{[]byte("3")}, d.GetMultimapState(linkID, wKey, uKey, []byte("b"))); diff != "" {
			t.Errorf("GetMultimapState(b) (-want, +got):\n%v", diff)
		}
		if diff := cmp.Diff([][]byte{[]byte("a"), []byte("b")}, sortedKeys(&d)); diff != "" {
			t.Errorf("GetMultimapKeysState before clear (-want, +got):\n%v", diff)
		}

		d.ClearMultimapState(linkID, wKey, uKey, []byte("a"))
		if got := d.GetMultimapState(linkID, wKey, uKey, []byte("a")); len(got) != 0 {
			t.Errorf("GetMultimapState(a) after clear = %v, want empty", got)
		}
		if diff := cmp.Diff([][]byte{[]byte("3")}, d.GetMultimapState(linkID, wKey, uKey, []byte("b"))); diff != "" {
			t.Errorf("GetMultimapState(b) after clear(a) (-want, +got):\n%v", diff)
		}
		if diff := cmp.Diff([][]byte{[]byte("b")}, sortedKeys(&d)); diff != "" {
			t.Errorf("GetMultimapKeysState after clear(a) (-want, +got):\n%v", diff)
		}

		d.ClearMultimapState(linkID, wKey, uKey, []byte("missing"))
		if diff := cmp.Diff([][]byte{[]byte("b")}, sortedKeys(&d)); diff != "" {
			t.Errorf("GetMultimapKeysState after clear(missing) (-want, +got):\n%v", diff)
		}

		d.ClearMultimapState(linkID, wKey, uKey, []byte("b"))
		if got := sortedKeys(&d); len(got) != 0 {
			t.Errorf("GetMultimapKeysState after clear(b) = %v, want empty", got)
		}
		if _, ok := d.state[linkID][window.GlobalWindow{}][string(uKey)]; !ok {
			t.Errorf("ClearMultimapState removed user key %q from tentative state", uKey)
		}
	})

	t.Run("clear_uninitialized", func(t *testing.T) {
		d := TentativeData{}
		d.ClearMultimapState(linkID, wKey, uKey, []byte("a"))
		if d.state != nil {
			t.Fatalf("ClearMultimapState on empty TentativeData allocated state map: %v", d.state)
		}
		if got := d.GetMultimapKeysState(linkID, wKey, uKey); len(got) != 0 {
			t.Fatalf("GetMultimapKeysState on empty TentativeData = %v, want empty", got)
		}

		otherLink := LinkID{Transform: "dofn", Local: "other"}
		d.AppendBagState(otherLink, wKey, uKey, []byte{1})
		d.ClearMultimapState(linkID, wKey, uKey, []byte("a"))
		if _, ok := d.state[linkID]; ok {
			t.Fatalf("ClearMultimapState created entry for uninitialized stateID %v", linkID)
		}

		d.AppendMultimapState(linkID, wKey, []byte("otherUser"), []byte("a"), []byte("1"))
		d.ClearMultimapState(linkID, wKey, uKey, []byte("a"))
		if _, ok := d.state[linkID][window.GlobalWindow{}][string(uKey)]; ok {
			t.Fatalf("ClearMultimapState created entry for uninitialized user key %q", uKey)
		}

		d.AppendMultimapState(linkID, wKey, uKey, []byte("a"), []byte("1"))
		d.ClearMultimapKeysState(linkID, wKey, uKey)
		d.ClearMultimapState(linkID, wKey, uKey, []byte("a"))
		if got := d.GetMultimapKeysState(linkID, wKey, uKey); len(got) != 0 {
			t.Errorf("GetMultimapKeysState after ClearMultimapKeysState = %v, want empty", got)
		}
	})

	t.Run("keys_and_bundle_state_skip_empty_entries", func(t *testing.T) {
		d := TentativeData{
			state: map[LinkID]map[typex.Window]map[string]StateData{
				linkID: {
					window.GlobalWindow{}: {
						string(uKey): {
							Multimap: map[string][][]byte{
								"nil_val":   nil,
								"empty_val": {},
								"active":    {[]byte("1")},
							},
						},
					},
				},
			},
		}
		if diff := cmp.Diff([][]byte{[]byte("active")}, sortedKeys(&d)); diff != "" {
			t.Errorf("GetMultimapKeysState with empty slices (-want, +got):\n%v", diff)
		}

		em := NewElementManager(Config{})
		em.AddStage("dofn", []string{"input"}, nil, nil)
		em.StageStateful("dofn", nil)
		ss := em.stages["dofn"]
		ss.inprogressKeysByBundle = map[string]set[string]{
			"b1": {"k1": {}, "k2": {}},
		}
		ss.state = map[LinkID]map[typex.Window]map[string]StateData{
			linkID: {
				window.GlobalWindow{}: {
					"k1": {
						Multimap: map[string][][]byte{
							"nil_val":   nil,
							"empty_val": {},
							"active":    {[]byte("v")},
						},
					},
					"k2": {
						Multimap: map[string][][]byte{
							"nil_val":   nil,
							"empty_val": {},
						},
					},
				},
			},
		}

		got := em.StateForBundle(RunBundle{StageID: "dofn", BundleID: "b1"})
		wantK1 := map[string][][]byte{"active": {[]byte("v")}}
		if diff := cmp.Diff(wantK1, got.state[linkID][window.GlobalWindow{}]["k1"].Multimap); diff != "" {
			t.Errorf("StateForBundle k1.Multimap (-want, +got):\n%v", diff)
		}
		if gotK2 := got.state[linkID][window.GlobalWindow{}]["k2"].Multimap; gotK2 != nil {
			t.Errorf("StateForBundle k2.Multimap = %v, want nil", gotK2)
		}
	})
}
