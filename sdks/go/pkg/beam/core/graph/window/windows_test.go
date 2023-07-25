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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

func TestWindowEquality(t *testing.T) {
	tests := []struct {
		name        string
		windowOne   typex.Window
		windowTwo   typex.Window
		expEquality bool
	}{
		{
			"global window == global window",
			GlobalWindow{},
			GlobalWindow{},
			true,
		},
		{
			"interval window[0,10] == interval window[0,10]",
			IntervalWindow{Start: 0, End: 10},
			IntervalWindow{Start: 0, End: 10},
			true,
		},
		{
			"interval window[0,10] == interval window[11 20]",
			IntervalWindow{Start: 0, End: 10},
			IntervalWindow{Start: 11, End: 20},
			false,
		},
		{
			"interval window[0,10] == interval window[0,20]",
			IntervalWindow{Start: 0, End: 10},
			IntervalWindow{Start: 0, End: 20},
			false,
		},
		{
			"interval window[0,10] == interval window[5,10]",
			IntervalWindow{Start: 0, End: 10},
			IntervalWindow{Start: 5, End: 10},
			false,
		},
		{
			"global window == interval window[0,10]",
			GlobalWindow{},
			IntervalWindow{Start: 0, End: 10},
			false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, want := test.windowOne.Equals(test.windowTwo), test.expEquality; got != want {
				t.Errorf("(%v).Equals(%v) got %v, want %v", test.windowOne, test.windowTwo, got, want)
			}
		})
	}
}

func TestMaxTimestamp(t *testing.T) {
	tests := []struct {
		name         string
		window       typex.Window
		expTimestamp typex.EventTime
	}{
		{
			"global window",
			GlobalWindow{},
			mtime.EndOfGlobalWindowTime,
		},
		{
			"interval window[0,100]",
			IntervalWindow{Start: 0, End: 100},
			99,
		},
		{
			"interval window[-100, 0]",
			IntervalWindow{Start: -100, End: 0},
			-1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, want := test.window.MaxTimestamp(), test.expTimestamp; got != want {
				t.Errorf("(%v).MaxTimestamp got %d, want %d", test.window, got, want)
			}
		})
	}
}

func TestIsEqualList(t *testing.T) {
	tests := []struct {
		name        string
		winsOne     []typex.Window
		winsTwo     []typex.Window
		expEquality bool
	}{
		{
			"equal lists",
			[]typex.Window{GlobalWindow{}, IntervalWindow{Start: 0, End: 10}, IntervalWindow{Start: 10, End: 20}},
			[]typex.Window{GlobalWindow{}, IntervalWindow{Start: 0, End: 10}, IntervalWindow{Start: 10, End: 20}},
			true,
		},
		{
			"out-of-order lists",
			[]typex.Window{GlobalWindow{}, IntervalWindow{Start: 0, End: 10}, IntervalWindow{Start: 10, End: 20}},
			[]typex.Window{IntervalWindow{Start: 0, End: 10}, GlobalWindow{}, IntervalWindow{Start: 10, End: 20}},
			false,
		},
		{
			"mismatched lengths",
			[]typex.Window{IntervalWindow{Start: 0, End: 10}, IntervalWindow{Start: 10, End: 20}},
			[]typex.Window{IntervalWindow{Start: 0, End: 10}, GlobalWindow{}, IntervalWindow{Start: 10, End: 20}},
			false,
		},
		{
			"different windows",
			[]typex.Window{GlobalWindow{}, IntervalWindow{Start: 0, End: 10}, IntervalWindow{Start: 10, End: 20}},
			[]typex.Window{GlobalWindow{}, IntervalWindow{Start: 0, End: 20}, IntervalWindow{Start: 20, End: 40}},
			false,
		},
		{
			"empty lists",
			[]typex.Window{},
			[]typex.Window{},
			true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, want := IsEqualList(test.winsOne, test.winsTwo), test.expEquality; got != want {
				t.Errorf("IsEqualList got %v, want %v", got, want)
			}
		})
	}
}
