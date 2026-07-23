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
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

func init() {
	RegisterWindowFn[*testWindowFn]()
}

func TestEquals(t *testing.T) {
	tests := []struct {
		name        string
		fnOne       *Fn
		fnTwo       *Fn
		expEquality bool
	}{
		{
			"global equal",
			NewGlobalWindows(),
			NewGlobalWindows(),
			true,
		},
		{
			"fixed equal",
			NewFixedWindows(100 * time.Millisecond),
			NewFixedWindows(100 * time.Millisecond),
			true,
		},
		{
			"fixed inequal duration",
			NewFixedWindows(100 * time.Millisecond),
			NewFixedWindows(200 * time.Millisecond),
			false,
		},
		{
			"sliding equal",
			NewSlidingWindows(10*time.Millisecond, 100*time.Millisecond),
			NewSlidingWindows(10*time.Millisecond, 100*time.Millisecond),
			true,
		},
		{
			"sliding inequal period",
			NewSlidingWindows(10*time.Millisecond, 100*time.Millisecond),
			NewSlidingWindows(20*time.Millisecond, 100*time.Millisecond),
			false,
		},
		{
			"sliding inequal duration",
			NewSlidingWindows(10*time.Millisecond, 100*time.Millisecond),
			NewSlidingWindows(10*time.Millisecond, 110*time.Millisecond),
			false,
		},
		{
			"session equal",
			NewSessions(10 * time.Minute),
			NewSessions(10 * time.Minute),
			true,
		},
		{
			"session inequal gap",
			NewSessions(5 * time.Minute),
			NewSessions(10 * time.Minute),
			false,
		},
		{
			"mismatched type",
			NewFixedWindows(100 * time.Millisecond),
			NewSlidingWindows(10*time.Millisecond, 100*time.Millisecond),
			false,
		},
		{
			"custom equal",
			NewCustom(&testWindowFn{BucketSize: 3000}),
			NewCustom(&testWindowFn{BucketSize: 3000}),
			true,
		},
		{
			"custom inequal",
			NewCustom(&testWindowFn{BucketSize: 3000}),
			NewCustom(&testWindowFn{BucketSize: 5000}),
			false,
		},
		{
			"custom vs fixed",
			NewCustom(&testWindowFn{BucketSize: 3000}),
			NewFixedWindows(3 * time.Second),
			false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, want := test.fnOne.Equals(test.fnTwo), test.expEquality; got != want {
				t.Errorf("(%v).Equals(%v) got %v, want %v", test.fnOne, test.fnTwo, got, want)
			}
		})
	}
}

// testWindowFn is a minimal custom WindowFn for testing.
type testWindowFn struct {
	BucketSize int64
}

func (f *testWindowFn) AssignWindows(ts typex.EventTime) []typex.Window {
	bucket := typex.EventTime(f.BucketSize)
	// Euclidean remainder; correct floor for negative ts.
	start := ts - ((ts%bucket)+bucket)%bucket
	end := start + bucket
	return []typex.Window{IntervalWindow{Start: start, End: end}}
}

func TestNewCustom(t *testing.T) {
	fn := NewCustom(&testWindowFn{BucketSize: 3000})
	if fn.Kind != CustomWindows {
		t.Errorf("NewCustom().Kind = %v, want %v", fn.Kind, CustomWindows)
	}
	if fn.CustomFn == nil {
		t.Fatal("NewCustom().CustomFn is nil")
	}
}

func TestNewCustomPanicsOnNil(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewCustom(nil) did not panic")
		}
	}()
	NewCustom(nil)
}

func TestCustomCoder(t *testing.T) {
	fn := NewCustom(&testWindowFn{BucketSize: 3000})
	got := fn.Coder()
	want := coder.NewIntervalWindow()
	if got.Kind != want.Kind {
		t.Errorf("Coder().Kind = %v, want %v", got.Kind, want.Kind)
	}
}

func TestCustomString(t *testing.T) {
	fn := NewCustom(&testWindowFn{BucketSize: 3000})
	s := fn.String()
	if !strings.HasPrefix(s, "CUS[") {
		t.Errorf("String() = %q, want prefix CUS[", s)
	}
}
