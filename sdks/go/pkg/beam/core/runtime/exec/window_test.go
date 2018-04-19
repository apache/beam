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
	"testing"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
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
	}

	for _, test := range tests {
		out := assignWindows(test.fn, test.in)
		if !window.IsEqualList(out, test.out) {
			t.Errorf("assignWindows(%v, %v) = %v, want %v", test.fn, test.in, out, test.out)
		}
	}
}
