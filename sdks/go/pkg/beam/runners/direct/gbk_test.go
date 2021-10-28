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

package direct

import (
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

func TestMergeWindows(t *testing.T) {
	tests := []struct {
		name            string
		wins            []typex.Window
		expectedMerge   []typex.Window
		expectedMapping []int
	}{
		{
			"two to one",
			[]typex.Window{window.IntervalWindow{Start: 0, End: 1000}, window.IntervalWindow{Start: 900, End: 1200}},
			[]typex.Window{window.IntervalWindow{Start: 0, End: 1200}},
			[]int{0, 0},
		},
		{
			"four to two",
			[]typex.Window{window.IntervalWindow{Start: 0, End: 1000}, window.IntervalWindow{Start: 900, End: 1200},
				window.IntervalWindow{Start: 1750, End: 1900}, window.IntervalWindow{Start: 1900, End: 2000}},
			[]typex.Window{window.IntervalWindow{Start: 0, End: 1200}, window.IntervalWindow{Start: 1750, End: 2000}},
			[]int{0, 0, 1, 1},
		},
		{
			"no merge",
			[]typex.Window{window.IntervalWindow{Start: 0, End: 800}, window.IntervalWindow{Start: 900, End: 1200}},
			[]typex.Window{window.IntervalWindow{Start: 0, End: 800}, window.IntervalWindow{Start: 900, End: 1200}},
			[]int{0, 1},
		},
		{
			"no windows",
			[]typex.Window{},
			[]typex.Window{},
			[]int{},
		},
	}
	for _, tc := range tests {
		c := CoGBK{wins: tc.wins}
		m, err := c.mergeWindows()
		if err != nil {
			t.Errorf("mergeWindows returned error, got %v", err)
		}
		if len(c.wins) != len(tc.expectedMerge) {
			t.Errorf("%v got %v windows instead of 1", tc.name, len(c.wins))
		}
		for i, win := range c.wins {
			if !win.Equals(tc.expectedMerge[i]) {
				t.Errorf("%v got window %v, expected %v", tc.name, win, tc.expectedMerge[i])
			}
			if m[win] != tc.expectedMapping[i] {
				t.Errorf("%v window %v mapped to index %v, expected %v", tc.name, win, m[win], tc.expectedMapping[i])
			}
		}
	}
}

func TestMergeWindows_BadType(t *testing.T) {
	c := CoGBK{wins: []typex.Window{window.GlobalWindow{}}}
	_, err := c.mergeWindows()
	if err == nil {
		t.Fatalf("mergeWindows() succeeded when it should have failed")
	}
	if !strings.Contains(err.Error(), "tried to merge non-interval window type window.GlobalWindow") {
		t.Errorf("mergeWindows failed but got incorrect error %v", err)
	}
}
