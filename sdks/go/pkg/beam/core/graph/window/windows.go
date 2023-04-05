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
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

var (
	// SingleGlobalWindow is a slice of a single global window. Convenience value.
	SingleGlobalWindow = []typex.Window{GlobalWindow{}}
)

// GlobalWindow represents the singleton, global window.
type GlobalWindow struct{}

// MaxTimestamp returns the maximum timestamp in the window.
func (GlobalWindow) MaxTimestamp() typex.EventTime {
	return mtime.EndOfGlobalWindowTime
}

// Equals returns a boolean indicating if the window is equal to
// a given window. This is true for global windows if the provided
// window is also a global window.
func (GlobalWindow) Equals(o typex.Window) bool {
	_, ok := o.(GlobalWindow)
	return ok
}

func (GlobalWindow) String() string {
	return "[*]"
}

// IntervalWindow represents a half-open bounded window [start,end).
type IntervalWindow struct {
	Start, End typex.EventTime
}

// MaxTimestamp returns the maximum timestamp in the window.
func (w IntervalWindow) MaxTimestamp() typex.EventTime {
	return typex.EventTime(mtime.Time(w.End).Milliseconds() - 1)
}

// Equals returns a boolean indicating if the window is equal to
// a given window. This is true for interval windows if the provided
// window is an interval window and they share the start and end
// timestamps.
func (w IntervalWindow) Equals(o typex.Window) bool {
	ow, ok := o.(IntervalWindow)
	return ok && w.Start == ow.Start && w.End == ow.End
}

func (w IntervalWindow) String() string {
	return fmt.Sprintf("[%v:%v)", w.Start, w.End)
}

// IsEqualList returns true iff the lists of windows are equal.
// Note that ordering matters and that this is not set equality.
func IsEqualList(from, to []typex.Window) bool {
	if len(from) != len(to) {
		return false
	}
	for i, w := range from {
		if !w.Equals(to[i]) {
			return false
		}
	}
	return true
}
