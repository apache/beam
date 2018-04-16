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

// Package window contains window representation, windowing strategies and utilities.
package window

import (
	"fmt"
	"time"
)

// TODO(herohde) 4/15/2018: separate WindowFn from WindowingStrategy.

// Kind is the semantic type of windowing strategy.
type Kind string

const (
	GlobalWindows  Kind = "GLO"
	FixedWindows   Kind = "FIX"
	SlidingWindows Kind = "SLI"
	Sessions       Kind = "SES"
)

// NewGlobalWindows returns the global windowing strategy, which places all elements
// into a single window.
func NewGlobalWindows() *WindowingStrategy {
	return &WindowingStrategy{Kind: GlobalWindows}
}

// NewFixedWindows returns the fixed windowing strategy with the given interval.
func NewFixedWindows(interval time.Duration) *WindowingStrategy {
	return &WindowingStrategy{Kind: FixedWindows, Size: interval}
}

// NewSlidingWindows returns the sliding windowing strategy with the given period and duration.
func NewSlidingWindows(period, duration time.Duration) *WindowingStrategy {
	return &WindowingStrategy{Kind: SlidingWindows, Period: period, Size: duration}
}

// NewSessions returns the session windowing strategy with the given gap.
func NewSessions(gap time.Duration) *WindowingStrategy {
	return &WindowingStrategy{Kind: Sessions, Gap: gap}
}

// WindowingStrategy defines the types of windowing used in a pipeline and contains
// the data to support executing a windowing strategy.
type WindowingStrategy struct {
	Kind Kind

	Size   time.Duration // FixedWindows, SlidingWindows
	Period time.Duration // SlidingWindows
	Gap    time.Duration // Sessions
}

func (w *WindowingStrategy) String() string {
	switch w.Kind {
	case FixedWindows:
		return fmt.Sprintf("%v[%v]", w.Kind, w.Size)
	case SlidingWindows:
		return fmt.Sprintf("%v[%v@%v]", w.Kind, w.Size, w.Period)
	case Sessions:
		return fmt.Sprintf("%v[%v]", w.Kind, w.Gap)
	default:
		return string(w.Kind)
	}
}

// Equals returns true iff the windows have the same kind and underlying behavior.
// Built-in window types (such as global window) are only equal to the same
// instances of the window. A user-defined window that happens to match a
// built-in will not match on Equals().
func (w *WindowingStrategy) Equals(o *WindowingStrategy) bool {
	if w.Kind != o.Kind {
		return false
	}

	switch w.Kind {
	case GlobalWindows:
		return true
	case FixedWindows:
		return w.Size == o.Size
	case SlidingWindows:
		return w.Period == o.Period && w.Size == o.Size
	case Sessions:
		return w.Gap == o.Gap
	default:
		panic(fmt.Sprintf("unknown window type: %v", w))
	}
}
