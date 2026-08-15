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

package window

import (
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

// elemAwareAnyWindowFn accepts an element typed as any — fast path 2.
type elemAwareAnyWindowFn struct {
	SizeMs int64
}

func (f *elemAwareAnyWindowFn) AssignWindows(ts typex.EventTime, _ any) []typex.Window {
	size := typex.EventTime(f.SizeMs)
	start := ts - (ts % size)
	return []typex.Window{IntervalWindow{Start: start, End: start + size}}
}

// elemAwareConcreteWindowFn accepts a concrete element type — reflect path.
type elemAwareConcreteWindowFn struct {
	DefaultSizeMs int64
}

func (f *elemAwareConcreteWindowFn) AssignWindows(ts typex.EventTime, elem int64) []typex.Window {
	size := typex.EventTime(elem)
	if size <= 0 {
		size = typex.EventTime(f.DefaultSizeMs)
	}
	start := ts - (ts % size)
	return []typex.Window{IntervalWindow{Start: start, End: start + size}}
}

func init() {
	RegisterWindowFn[*elemAwareAnyWindowFn]()
	RegisterWindowFn[*elemAwareConcreteWindowFn]()
}

func TestWindowFnInvoker_TimestampOnly(t *testing.T) {
	fn := &testWindowFn{BucketSize: 3000}
	inv := NewWindowFnInvoker(fn)

	if inv.NeedsElement() {
		t.Fatal("NeedsElement() = true, want false")
	}

	windows := inv.Invoke(1500, nil)
	if len(windows) != 1 {
		t.Fatalf("got %d windows, want 1", len(windows))
	}
	want := IntervalWindow{Start: 0, End: 3000}
	if !windows[0].Equals(want) {
		t.Errorf("Invoke(1500, nil) = %v, want %v", windows[0], want)
	}
}

func TestWindowFnInvoker_AnyElem(t *testing.T) {
	fn := &elemAwareAnyWindowFn{SizeMs: 5000}
	inv := NewWindowFnInvoker(fn)

	if !inv.NeedsElement() {
		t.Fatal("NeedsElement() = false, want true")
	}

	windows := inv.Invoke(7500, "ignored")
	if len(windows) != 1 {
		t.Fatalf("got %d windows, want 1", len(windows))
	}
	want := IntervalWindow{Start: 5000, End: 10000}
	if !windows[0].Equals(want) {
		t.Errorf("Invoke(7500, ignored) = %v, want %v", windows[0], want)
	}
}

func TestWindowFnInvoker_ConcreteElem(t *testing.T) {
	fn := &elemAwareConcreteWindowFn{DefaultSizeMs: 1000}
	inv := NewWindowFnInvoker(fn)

	if !inv.NeedsElement() {
		t.Fatal("NeedsElement() = false, want true")
	}

	// Element provides window size of 5000ms.
	windows := inv.Invoke(7500, int64(5000))
	if len(windows) != 1 {
		t.Fatalf("got %d windows, want 1", len(windows))
	}
	want := IntervalWindow{Start: 5000, End: 10000}
	if !windows[0].Equals(want) {
		t.Errorf("Invoke(7500, 5000) = %v, want %v", windows[0], want)
	}

	// Element <= 0: falls back to default.
	windows = inv.Invoke(1500, int64(0))
	if len(windows) != 1 {
		t.Fatalf("got %d windows, want 1", len(windows))
	}
	want = IntervalWindow{Start: 1000, End: 2000}
	if !windows[0].Equals(want) {
		t.Errorf("Invoke(1500, 0) = %v, want %v", windows[0], want)
	}
}

func TestWindowFnInvoker_NeedsElementCorrectness(t *testing.T) {
	tests := []struct {
		name string
		fn   any
		want bool
	}{
		{"timestamp-only", &testWindowFn{BucketSize: 1000}, false},
		{"any-elem", &elemAwareAnyWindowFn{SizeMs: 1000}, true},
		{"concrete-elem", &elemAwareConcreteWindowFn{DefaultSizeMs: 1000}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inv := NewWindowFnInvoker(tc.fn)
			if got := inv.NeedsElement(); got != tc.want {
				t.Errorf("NeedsElement() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestWindowFnInvoker_PanicOnUnregistered(t *testing.T) {
	type unregisteredFn struct{}
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewWindowFnInvoker did not panic on unregistered type")
		}
	}()
	NewWindowFnInvoker(&unregisteredFn{})
}
