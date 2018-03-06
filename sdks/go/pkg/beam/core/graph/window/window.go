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

// Package window contains window representation and utilities.
package window

import "fmt"

// Window defines the types of windowing used in a pipeline and contains
// the data and code to support executing a windowing strategy.
type Window struct {
	k Kind
	// TODO: pointer to windowing function
	// TODO: other fields
}

// Kind is the semantic type of window.
type Kind string

const (
	// GlobalWindow is the default window into which all elements are placed.
	GlobalWindow Kind = "GW"
)

// NewGlobalWindow returns the default window to be used for a collection.
func NewGlobalWindow() *Window {
	return &Window{k: GlobalWindow}
}
func (w *Window) String() string {
	return string(w.k)
}

// Kind returns the kind of the window.
func (w *Window) Kind() Kind {
	return w.k
}

// Equals returns true iff the windows have the same kind and underlying behavior.
// Built-in window types (such as global window) are only equal to the same
// instances of the window. A user-defined window that happens to match a
// built-in will not match on Equals().
func (w *Window) Equals(o *Window) bool {
	switch w.Kind() {
	case GlobalWindow:
		return o.Kind() == w.Kind()
	default:
		panic(fmt.Sprintf("unknown window type: %v", w))
		// TODO(wcn): implement other window types
	}
}

// TODO: Sessions, FixedWindows, etc.

// CustomWindow is the base-case windowing that relies on a user
// specified function for windowing behavior
// var CustomWindow WindowKind

// TODO(wcn): establish the other window types when we are
// ready to support them.
