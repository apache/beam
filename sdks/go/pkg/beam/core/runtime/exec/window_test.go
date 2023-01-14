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
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

// TestAssignWindow tests that each window fn assigns the
// correct windows for a given timestamp.
func TestAssignWindow(t *testing.T) {
	tests := []struct {
		fn  *window.Fn
		in  typex.EventTime
		out []typex.Window
	}{
		{
			window.NewGlobalWindows(),
			mtime.ZeroTimestamp,
			window.SingleGlobalWindow,
		},
		{
			window.NewGlobalWindows(),
			mtime.MinTimestamp,
			window.SingleGlobalWindow,
		},
		{
			window.NewGlobalWindows(),
			mtime.Now(),
			window.SingleGlobalWindow,
		},
		{
			window.NewGlobalWindows(),
			mtime.MaxTimestamp, // TODO(herohde) 4/18/2018: is this even valid?
			window.SingleGlobalWindow,
		},
		{
			window.NewFixedWindows(time.Minute),
			0,
			[]typex.Window{window.IntervalWindow{Start: 0, End: 60000}},
		},
		{
			window.NewFixedWindows(time.Minute),
			-123,
			[]typex.Window{window.IntervalWindow{Start: -60000, End: 0}},
		},
		{
			window.NewFixedWindows(time.Minute),
			59999,
			[]typex.Window{window.IntervalWindow{Start: 0, End: 60000}},
		},
		{
			window.NewFixedWindows(time.Minute),
			60000,
			[]typex.Window{window.IntervalWindow{Start: 60000, End: 120000}},
		},
		{
			window.NewFixedWindows(2 * time.Minute),
			60000,
			[]typex.Window{window.IntervalWindow{Start: 0, End: 120000}},
		},
		{
			window.NewSlidingWindows(time.Minute, 3*time.Minute),
			0,
			[]typex.Window{
				window.IntervalWindow{Start: 0, End: 180000},
				window.IntervalWindow{Start: -60000, End: 120000},
				window.IntervalWindow{Start: -120000, End: 60000},
			},
		},
		{
			window.NewSlidingWindows(time.Minute, 3*time.Minute),
			123,
			[]typex.Window{
				window.IntervalWindow{Start: 0, End: 180000},
				window.IntervalWindow{Start: -60000, End: 120000},
				window.IntervalWindow{Start: -120000, End: 60000},
			},
		},
		{
			window.NewSlidingWindows(time.Minute, 3*time.Minute),
			60000,
			[]typex.Window{
				window.IntervalWindow{Start: 60000, End: 240000},
				window.IntervalWindow{Start: 0, End: 180000},
				window.IntervalWindow{Start: -60000, End: 120000},
			},
		},
		{
			window.NewSessions(time.Minute),
			60000,
			[]typex.Window{
				window.IntervalWindow{Start: 60000, End: 120000},
			},
		},
	}

	for _, test := range tests {
		out := assignWindows(test.fn, test.in)
		if !window.IsEqualList(out, test.out) {
			t.Errorf("assignWindows(%v, %v) = %v, want %v", test.fn, test.in, out, test.out)
		}
	}
}

func TestMapWindow(t *testing.T) {
	tests := []struct {
		name     string
		wfn      *window.Fn
		in       typex.Window
		expected typex.Window
	}{
		{
			"interval to global",
			window.NewGlobalWindows(),
			window.IntervalWindow{Start: 0, End: 1000},
			window.GlobalWindow{},
		},
		{
			"global to global",
			window.NewGlobalWindows(),
			window.GlobalWindow{},
			window.GlobalWindow{},
		},
		{
			"interval to interval",
			window.NewFixedWindows(1000 * time.Millisecond),
			window.IntervalWindow{Start: 0, End: 100},
			window.IntervalWindow{Start: 0, End: 1000},
		},
		{
			"interval to sliding within first",
			window.NewSlidingWindows(300*time.Millisecond, 1000*time.Millisecond),
			window.IntervalWindow{Start: 0, End: 999},
			window.IntervalWindow{Start: 0, End: 1000},
		},
		{
			"interval to sliding beyond first",
			window.NewSlidingWindows(300*time.Millisecond, 1000*time.Millisecond),
			window.IntervalWindow{Start: 0, End: 1001},
			window.IntervalWindow{Start: 300, End: 1300},
		},
	}
	for _, test := range tests {
		mapper := &windowMapper{wfn: test.wfn}
		outputWin, err := mapper.MapWindow(test.in)
		if err != nil {
			t.Fatalf("MapWindow for test %v failed, got %v", test.name, err)
		}
		if !outputWin.Equals(test.expected) {
			t.Errorf("test %v failed: expected window %v, got %v", test.name, test.expected, outputWin)
		}
	}
}

func TestMapWindows(t *testing.T) {
	tests := []struct {
		name   string
		wFn    *window.Fn
		in     []typex.Window
		expect []typex.Window
	}{
		{
			"fixed2fixed",
			window.NewFixedWindows(1000 * time.Millisecond),
			[]typex.Window{
				window.IntervalWindow{Start: 100, End: 200},
				window.IntervalWindow{Start: 100, End: 1100},
			},
			[]typex.Window{
				window.IntervalWindow{Start: 0, End: 1000},
				window.IntervalWindow{Start: 1000, End: 2000},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inV, expected := makeNoncedWindowValues(tc.in, tc.expect)

			out := &CaptureNode{UID: 1}
			unit := &MapWindows{UID: 2, Fn: &windowMapper{wfn: tc.wFn}, Out: out}
			a := &FixedRoot{UID: 3, Elements: inV, Out: unit}

			p, err := NewPlan(tc.name, []Unit{a, unit, out})
			if err != nil {
				t.Fatalf("failed to construct plan: %s", err)
			}
			ctx := context.Background()
			if err := p.Execute(ctx, "1", DataContext{}); err != nil {
				t.Fatalf("execute failed: %s", err)
			}
			if err := p.Down(ctx); err != nil {
				t.Fatalf("down failed: %s", err)
			}
			if !equalList(out.Elements, expected) {
				t.Errorf("map_windows returned %v, want %v", extractValues(out.Elements...), extractValues(expected...))
			}
		})
	}
}

func makeNoncedWindowValues(in []typex.Window, expect []typex.Window) ([]MainInput, []FullValue) {
	if len(in) != len(expect) {
		panic("provided window slices must be the same length")
	}
	inV := make([]MainInput, len(in))
	expectV := make([]FullValue, len(in))
	for i := range in {
		nonce := make([]byte, 4)
		rand.Read(nonce)
		inV[i] = MainInput{Key: makeKV(nonce, in[i])[0]}
		expectV[i] = makeKV(nonce, expect[i])[0]
	}
	return inV, expectV
}
