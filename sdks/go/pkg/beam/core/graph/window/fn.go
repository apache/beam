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
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
)

// Kind is the semantic type of a window fn.
type Kind string

const (
	GlobalWindows  Kind = "GLO"
	FixedWindows   Kind = "FIX"
	SlidingWindows Kind = "SLI"
	Sessions       Kind = "SES"
	CustomWindows  Kind = "CUS" // User-defined custom WindowFn
)

// NewGlobalWindows returns the default WindowFn, which places all elements
// into a single window.
func NewGlobalWindows() *Fn {
	return &Fn{Kind: GlobalWindows}
}

// NewFixedWindows returns the fixed WindowFn with the given interval.
func NewFixedWindows(interval time.Duration) *Fn {
	return &Fn{Kind: FixedWindows, Size: interval}
}

// NewSlidingWindows returns the sliding WindowFn with the given period and duration.
func NewSlidingWindows(period, duration time.Duration) *Fn {
	return &Fn{Kind: SlidingWindows, Period: period, Size: duration}
}

// NewSessions returns the session WindowFn with the given gap.
func NewSessions(gap time.Duration) *Fn {
	return &Fn{Kind: Sessions, Gap: gap}
}

// NewCustom returns a WindowFn backed by a user-defined custom window
// function. The fn value must be a pointer-to-struct whose concrete type
// has been registered with [RegisterWindowFn] during init. Custom window
// functions are non-merging and must return [IntervalWindow] values from
// their AssignWindows method.
//
// NewCustom panics if fn is nil or its type was not registered.
func NewCustom(fn any) *Fn {
	if fn == nil {
		panic("window.NewCustom: fn must not be nil")
	}
	t := reflect.TypeOf(fn)
	st := t
	if t.Kind() == reflect.Pointer {
		st = t.Elem()
	}
	if LookupWindowFnMeta(st) == nil {
		panic(fmt.Sprintf("window.NewCustom: type %v is not registered; call window.RegisterWindowFn during init()", t))
	}
	return &Fn{Kind: CustomWindows, CustomFn: fn}
}

// Fn defines the window fn.
type Fn struct {
	Kind Kind

	Size   time.Duration // FixedWindows, SlidingWindows
	Period time.Duration // SlidingWindows
	Gap    time.Duration // Sessions

	CustomFn any // CustomWindows (nil for built-in kinds)
}

// NeedsElement reports whether a CustomWindows Fn has an element-aware
// AssignWindows signature. Returns false for all built-in window kinds.
func (w *Fn) NeedsElement() bool {
	if w.Kind != CustomWindows || w.CustomFn == nil {
		return false
	}
	t := reflect.TypeOf(w.CustomFn)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if meta := LookupWindowFnMeta(t); meta != nil {
		return meta.NeedsElement()
	}
	return false
}

// TODO(herohde) 4/17/2018: do we need to expose the window type as well?

// Coder returns the WindowCoder for the WindowFn.
func (w *Fn) Coder() *coder.WindowCoder {
	switch w.Kind {
	case GlobalWindows:
		return coder.NewGlobalWindow()
	default:
		return coder.NewIntervalWindow()
	}
}

func (w *Fn) String() string {
	switch w.Kind {
	case FixedWindows:
		return fmt.Sprintf("%v[%v]", w.Kind, w.Size)
	case SlidingWindows:
		return fmt.Sprintf("%v[%v@%v]", w.Kind, w.Size, w.Period)
	case Sessions:
		return fmt.Sprintf("%v[%v]", w.Kind, w.Gap)
	case CustomWindows:
		return fmt.Sprintf("%v[%v]", w.Kind, reflect.TypeOf(w.CustomFn))
	default:
		return string(w.Kind)
	}
}

// Equals returns true iff the windows have the same kind and underlying behavior.
// Built-in window types (such as global window) are only equal to the same
// instances of the window. A user-defined window that happens to match a
// built-in will not match on Equals().
func (w *Fn) Equals(o *Fn) bool {
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
	case CustomWindows:
		return reflect.DeepEqual(w.CustomFn, o.CustomFn)
	default:
		panic(fmt.Sprintf("unknown window type: %v", w))
	}
}
