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

import "fmt"

// WindowingStrategy defines the types of windowing used in a pipeline and contains
// the data and code to support executing a windowing strategy.
type WindowingStrategy struct {
	k Kind
	// TODO: pointer to windowing function
	// TODO: other fields
}

// Kind is the semantic type of windowing strategy.
type Kind string

const (
	// GlobalWindows is the default windowing strategy, which places all elements
	// into a single window.
	GlobalWindows Kind = "GLO"
)

// NewGlobalWindows returns the default window to be used for a collection.
func NewGlobalWindows() *WindowingStrategy {
	return &WindowingStrategy{k: GlobalWindows}
}

func (w *WindowingStrategy) String() string {
	return string(w.k)
}

// Kind returns the kind of the windowing strategy.
func (w *WindowingStrategy) Kind() Kind {
	return w.k
}

// Equals returns true iff the windows have the same kind and underlying behavior.
// Built-in window types (such as global window) are only equal to the same
// instances of the window. A user-defined window that happens to match a
// built-in will not match on Equals().
func (w *WindowingStrategy) Equals(o *WindowingStrategy) bool {
	switch w.Kind() {
	case GlobalWindows:
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
