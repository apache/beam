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
	"context"
	"fmt"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/google/go-cmp/cmp"
)

func TestMakeTestStreamHandler_ProcessingTime(t *testing.T) {
	before := mtime.Now()
	ts := makeTestStreamHandler("ts")
	after := mtime.Now()

	if ts.processingTime.IsZero() {
		t.Fatal("makeTestStreamHandler left processingTime at zero time.Time")
	}
	if got := ts.Now(); got < before || got > after {
		t.Errorf("ts.Now() = %v, want in [%v, %v]", got, before, after)
	}
}

func testStreamPColInfo(t *testing.T, globalID string, keyed bool) PColInfo {
	t.Helper()
	readAll := func(r io.Reader) []byte {
		b, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("read element: %v", err)
		}
		return b
	}
	info := PColInfo{
		GlobalID:    globalID,
		WindowCoder: WinGlobal,
		WDec:        exec.MakeWindowDecoder(coder.NewGlobalWindow()),
		WEnc:        exec.MakeWindowEncoder(coder.NewGlobalWindow()),
		EDec:        readAll,
	}
	if keyed {
		info.KeyDec = func(r io.Reader) []byte {
			var k [1]byte
			if _, err := io.ReadFull(r, k[:]); err != nil {
				t.Fatalf("read key: %v", err)
			}
			return k[:]
		}
	}
	return info
}

func encodeWindowedBytes(t *testing.T, info PColInfo, et mtime.Time, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := exec.EncodeWindowedValueHeader(info.WEnc, []typex.Window{window.GlobalWindow{}}, et, typex.NoFiringPane(), &buf); err != nil {
		t.Fatalf("EncodeWindowedValueHeader: %v", err)
	}
	buf.Write(payload)
	return buf.Bytes()
}

func encodeGlobalTimerBytes(t *testing.T, info PColInfo, keyBytes []byte, firing, hold mtime.Time) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.Write(keyBytes)
	if err := coder.EncodeVarInt(0, &buf); err != nil {
		t.Fatalf("EncodeVarInt(tag): %v", err)
	}
	if err := info.WEnc.Encode([]typex.Window{window.GlobalWindow{}}, &buf); err != nil {
		t.Fatalf("WEnc.Encode: %v", err)
	}
	buf.WriteByte(0)
	if err := coder.EncodeEventTime(firing, &buf); err != nil {
		t.Fatalf("EncodeEventTime(firing): %v", err)
	}
	if err := coder.EncodeEventTime(hold, &buf); err != nil {
		t.Fatalf("EncodeEventTime(hold): %v", err)
	}
	if err := coder.EncodePane(typex.NoFiringPane(), &buf); err != nil {
		t.Fatalf("EncodePane: %v", err)
	}
	return buf.Bytes()
}

func TestTestStream_StatefulProcessingTimeTimer(t *testing.T) {
	tests := []struct {
		name             string
		viaMap           bool
		initialWatermark mtime.Time
	}{
		{name: "direct", initialWatermark: mtime.MinTimestamp},
		{name: "via_ordinary_stage", viaMap: true, initialWatermark: mtime.MinTimestamp},
		{name: "held_watermark", viaMap: true, initialWatermark: mtime.FromMilliseconds(100)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancelFn := context.WithCancelCause(context.Background())
			defer cancelFn(nil)

			em := NewElementManager(Config{})
			mapInInfo := testStreamPColInfo(t, "map_in", false)
			statefulInInfo := testStreamPColInfo(t, "stateful_in", true)
			sinkInInfo := testStreamPColInfo(t, "sink_in", false)

			tsOut := "stateful_in"
			if test.viaMap {
				tsOut = "map_in"
				em.RegisterPColInfo("map_in", mapInInfo)
			}
			em.RegisterPColInfo("stateful_in", statefulInInfo)
			em.RegisterPColInfo("sink_in", sinkInInfo)

			em.AddStage("teststream", nil, []string{tsOut}, nil)
			if test.viaMap {
				em.AddStage("map", []string{"map_in"}, []string{"stateful_in"}, nil)
			}
			em.AddStage("stateful", []string{"stateful_in"}, []string{"sink_in"}, nil)
			em.StageStateful("stateful", nil)
			em.StageProcessingTimeTimers("stateful", map[string]bool{"flush": true})
			em.AddStage("sink", []string{"sink_in"}, nil, nil)

			tsb := em.AddTestStream("teststream", map[string]string{"out": tsOut})
			if test.initialWatermark > mtime.MinTimestamp {
				tsb.AddWatermarkEvent("out", test.initialWatermark)
			}
			eventTime := mtime.FromMilliseconds(200)
			tsb.AddElementEvent("out", []TestStreamElement{
				{Encoded: []byte{'k', 1}, EventTime: eventTime},
				{Encoded: []byte{'k', 2}, EventTime: eventTime},
			})
			tsb.AddProcessingTimeEvent(60 * time.Second)
			tsb.AddElementEvent("out", []TestStreamElement{
				{Encoded: []byte{'k', 3}, EventTime: eventTime},
			})
			tsb.AddWatermarkEvent("out", mtime.MaxTimestamp)

			var nextID int
			ch := em.Bundles(ctx, cancelFn, func() string {
				nextID++
				return fmt.Sprintf("b%d", nextID)
			})

			var buf []byte
			var gotBatches [][]byte
			var sinkElements int

			for rb := range ch {
				switch rb.StageID {
				case "map":
					var td TentativeData
					for _, raw := range em.InputForBundle(rb, mapInInfo) {
						r := bytes.NewBuffer(raw)
						_, et, _, err := exec.DecodeWindowedValueHeader(mapInInfo.WDec, r)
						if err != nil {
							t.Fatalf("DecodeWindowedValueHeader(map): %v", err)
						}
						elm := mapInInfo.EDec(r)
						td.WriteData("stateful_in", encodeWindowedBytes(t, statefulInInfo, et, elm))
					}
					em.PersistBundle(rb, map[string]PColInfo{"stateful_in": statefulInInfo}, td, mapInInfo, Residuals{})

				case "stateful":
					blocks, _ := em.DataAndTimerInputForBundle(rb, statefulInInfo)
					var td TentativeData
					for _, block := range blocks {
						switch block.Kind {
						case BlockData:
							for _, raw := range block.Bytes {
								r := bytes.NewBuffer(raw)
								ws, et, _, err := exec.DecodeWindowedValueHeader(statefulInInfo.WDec, r)
								if err != nil {
									t.Fatalf("DecodeWindowedValueHeader(stateful): %v", err)
								}
								elm := statefulInInfo.EDec(r)
								if len(buf) == 0 {
									ptFire := mtime.Now().Add(10 * time.Second)
									td.WriteTimers("stateful", "flush", encodeGlobalTimerBytes(t, statefulInInfo, elm[:1], ptFire, et))
									winEnd := ws[0].MaxTimestamp()
									td.WriteTimers("stateful", "end", encodeGlobalTimerBytes(t, statefulInInfo, elm[:1], winEnd, winEnd))
								}
								buf = append(buf, elm[1])
							}
						case BlockTimer:
							if len(buf) > 0 {
								batch := slices.Clone(buf)
								slices.Sort(batch)
								gotBatches = append(gotBatches, batch)
								td.WriteData("sink_in", encodeWindowedBytes(t, sinkInInfo, eventTime, batch))
								buf = buf[:0]
							}
						}
					}
					em.PersistBundle(rb, map[string]PColInfo{"sink_in": sinkInInfo}, td, statefulInInfo, Residuals{})

				case "sink":
					sinkElements += len(em.InputForBundle(rb, sinkInInfo))
					em.PersistBundle(rb, nil, TentativeData{}, sinkInInfo, Residuals{})

				default:
					t.Fatalf("unexpected stage %q", rb.StageID)
				}
			}

			wantBatches := [][]byte{{1, 2}, {3}}
			if diff := cmp.Diff(wantBatches, gotBatches); diff != "" {
				t.Errorf("batches diff (-want +got):\n%s", diff)
			}
			if sinkElements != len(wantBatches) {
				t.Errorf("sink received %d elements, want %d", sinkElements, len(wantBatches))
			}
		})
	}
}
