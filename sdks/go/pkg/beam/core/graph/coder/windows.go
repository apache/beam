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

package coder

// WindowKind represents a kind of window coder.
type WindowKind string

const (
	GlobalWindow   WindowKind = "GWC"
	IntervalWindow WindowKind = "IWC"
)

// WindowCoder represents a Window coder.
type WindowCoder struct {
	Kind WindowKind
}

func (w *WindowCoder) Equals(o *WindowCoder) bool {
	return w.Kind == o.Kind
}

func (w *WindowCoder) String() string {
	return string(w.Kind)
}

// NewGlobalWindow returns a window coder for the global window.
func NewGlobalWindow() *WindowCoder {
	return &WindowCoder{Kind: GlobalWindow}
}

// NewIntervalWindow returns a window coder for interval windows.
func NewIntervalWindow() *WindowCoder {
	return &WindowCoder{Kind: IntervalWindow}
}
