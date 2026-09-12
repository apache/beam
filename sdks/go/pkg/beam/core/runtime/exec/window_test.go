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
		{
			// Custom window that mimics 3-second fixed windows.
			window.NewCustom(&fixedCustomWindowFn{SizeMs: 3000}),
			0,
			[]typex.Window{
				window.IntervalWindow{Start: 0, End: 3000},
			},
		},
		{
			window.NewCustom(&fixedCustomWindowFn{SizeMs: 3000}),
			2999,
			[]typex.Window{
				window.IntervalWindow{Start: 0, End: 3000},
			},
		},
		{
			window.NewCustom(&fixedCustomWindowFn{SizeMs: 3000}),
			3000,
			[]typex.Window{
				window.IntervalWindow{Start: 3000, End: 6000},
			},
		},
	}

	for _, test := range tests {
		out := assignWindows(test.fn, invokerFor(test.fn), test.in, nil, nil)
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
		mapper := newWindowMapper(test.wfn)
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
			unit := &MapWindows{UID: 2, Fn: newWindowMapper(tc.wFn), Out: out}
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

func init() {
	window.RegisterWindowFn[*fixedCustomWindowFn]()
	window.RegisterWindowFn[*elemSizedWindowFn]()
	window.RegisterWindowFn[*multiWindowFn]()
	window.RegisterWindowFn[*kvSizedWindowFn]()
}

// kvSizedWindowFn derives the window size from a KV element's value.
type kvSizedWindowFn struct{}

func (f *kvSizedWindowFn) AssignWindows(ts typex.EventTime, k string, v int64) []typex.Window {
	size := typex.EventTime(v)
	start := ts - ((ts%size)+size)%size
	return []typex.Window{window.IntervalWindow{Start: start, End: start + size}}
}

// TestWindowIntoKV checks that a KV element reaches AssignWindows as a
// separate key and value rather than the key alone.
func TestWindowIntoKV(t *testing.T) {
	tests := []struct {
		name string
		ts   typex.EventTime
		key  string
		val  int64
		want typex.Window
	}{
		{"value sets 3s size", 1500, "a", 3000, window.IntervalWindow{Start: 0, End: 3000}},
		{"value sets 6s size", 1500, "b", 6000, window.IntervalWindow{Start: 0, End: 6000}},
		{"value selects later window", 4500, "c", 3000, window.IntervalWindow{Start: 3000, End: 6000}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			out := &CaptureNode{UID: 1}
			wi := &WindowInto{UID: 2, Fn: window.NewCustom(&kvSizedWindowFn{}), Out: out}
			root := &FixedRoot{UID: 3, Elements: []MainInput{{Key: FullValue{
				Windows:   window.SingleGlobalWindow,
				Timestamp: test.ts,
				Elm:       test.key,
				Elm2:      test.val,
			}}}, Out: wi}

			p, err := NewPlan("a", []Unit{root, wi, out})
			if err != nil {
				t.Fatalf("failed to construct plan: %v", err)
			}
			if err := p.Execute(ctx, "1", DataContext{}); err != nil {
				t.Fatalf("execute failed: %v", err)
			}
			if err := p.Down(ctx); err != nil {
				t.Fatalf("down failed: %v", err)
			}

			if len(out.Elements) != 1 {
				t.Fatalf("got %v elements, want 1", len(out.Elements))
			}
			if got := out.Elements[0].Windows; !window.IsEqualList(got, []typex.Window{test.want}) {
				t.Errorf("WindowInto assigned %v, want %v", got, test.want)
			}
		})
	}
}

// multiWindowFn assigns every timestamp to two windows, earliest first.
type multiWindowFn struct{}

func (f *multiWindowFn) AssignWindows(ts typex.EventTime) []typex.Window {
	return []typex.Window{
		window.IntervalWindow{Start: 0, End: 1000},
		window.IntervalWindow{Start: 1000, End: 2000},
	}
}

// TestMapWindowCustom checks that side input mapping accepts a custom
// WindowFn only when it assigns to exactly one window. Picking among several
// candidates relies on an ordering only the built-in kinds guarantee.
func TestMapWindowCustom(t *testing.T) {
	tests := []struct {
		name    string
		wfn     *window.Fn
		in      typex.Window
		want    typex.Window
		wantErr bool
	}{
		{
			name: "single window",
			wfn:  window.NewCustom(&fixedCustomWindowFn{SizeMs: 1000}),
			in:   window.IntervalWindow{Start: 100, End: 200},
			want: window.IntervalWindow{Start: 0, End: 1000},
		},
		{
			name:    "multiple windows rejected",
			wfn:     window.NewCustom(&multiWindowFn{}),
			in:      window.IntervalWindow{Start: 100, End: 200},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := newWindowMapper(tc.wfn).MapWindow(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("MapWindow(%v) = %v, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("MapWindow(%v) failed: %v", tc.in, err)
			}
			if !got.Equals(tc.want) {
				t.Errorf("MapWindow(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// elemSizedWindowFn derives the window size from the element value.
type elemSizedWindowFn struct{}

func (f *elemSizedWindowFn) AssignWindows(ts typex.EventTime, elem int64) []typex.Window {
	size := typex.EventTime(elem)
	start := ts - ((ts%size)+size)%size
	return []typex.Window{window.IntervalWindow{Start: start, End: start + size}}
}

func BenchmarkAssignWindowsCustom(b *testing.B) {
	fn := window.NewCustom(&fixedCustomWindowFn{SizeMs: 3000})
	inv := invokerFor(fn)
	b.ReportAllocs()
	for b.Loop() {
		assignWindows(fn, inv, 1500, nil, nil)
	}
}

// TestWindowIntoElementAware checks that an element-aware custom WindowFn
// receives the element value rather than the enclosing FullValue.
func TestWindowIntoElementAware(t *testing.T) {
	tests := []struct {
		name string
		ts   typex.EventTime
		elm  int64
		want typex.Window
	}{
		{"element sets 3s size", 1500, 3000, window.IntervalWindow{Start: 0, End: 3000}},
		{"element sets 6s size", 1500, 6000, window.IntervalWindow{Start: 0, End: 6000}},
		{"element selects later window", 4500, 3000, window.IntervalWindow{Start: 3000, End: 6000}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			out := &CaptureNode{UID: 1}
			wi := &WindowInto{UID: 2, Fn: window.NewCustom(&elemSizedWindowFn{}), Out: out}
			root := &FixedRoot{UID: 3, Elements: []MainInput{{Key: FullValue{
				Windows:   window.SingleGlobalWindow,
				Timestamp: test.ts,
				Elm:       test.elm,
			}}}, Out: wi}

			p, err := NewPlan("a", []Unit{root, wi, out})
			if err != nil {
				t.Fatalf("failed to construct plan: %v", err)
			}
			if err := p.Execute(ctx, "1", DataContext{}); err != nil {
				t.Fatalf("execute failed: %v", err)
			}
			if err := p.Down(ctx); err != nil {
				t.Fatalf("down failed: %v", err)
			}

			if len(out.Elements) != 1 {
				t.Fatalf("got %v elements, want 1", len(out.Elements))
			}
			if got := out.Elements[0].Windows; !window.IsEqualList(got, []typex.Window{test.want}) {
				t.Errorf("WindowInto assigned %v, want %v", got, test.want)
			}
		})
	}
}

// fixedCustomWindowFn is a test custom WindowFn that mimics fixed windows.
type fixedCustomWindowFn struct {
	SizeMs int64
}

func (f *fixedCustomWindowFn) AssignWindows(ts typex.EventTime) []typex.Window {
	size := typex.EventTime(f.SizeMs)
	start := ts - (ts % size)
	if ts < 0 {
		// Go's % truncates toward zero, so for negative dividends
		// ts%size is non-positive and ts-(ts%size) rounds toward
		// zero instead of toward -inf. The double-mod expression
		// computes the Euclidean (non-negative) remainder, giving
		// a correct floor to the window boundary.
		start = ts - (ts%size+size)%size
	}
	return []typex.Window{window.IntervalWindow{Start: start, End: start + size}}
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
