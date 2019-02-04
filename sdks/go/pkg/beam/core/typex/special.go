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

package typex

import (
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
)

// This file defines data types that programs use to indicate a
// data value is representing a particular Beam concept.

var (
	TType = reflect.TypeOf((*T)(nil)).Elem()
	UType = reflect.TypeOf((*U)(nil)).Elem()
	VType = reflect.TypeOf((*V)(nil)).Elem()
	WType = reflect.TypeOf((*W)(nil)).Elem()
	XType = reflect.TypeOf((*X)(nil)).Elem()
	YType = reflect.TypeOf((*Y)(nil)).Elem()
	ZType = reflect.TypeOf((*Z)(nil)).Elem()

	EventTimeType = reflect.TypeOf((*EventTime)(nil)).Elem()
	WindowType    = reflect.TypeOf((*Window)(nil)).Elem()

	KVType            = reflect.TypeOf((*KV)(nil)).Elem()
	CoGBKType         = reflect.TypeOf((*CoGBK)(nil)).Elem()
	WindowedValueType = reflect.TypeOf((*WindowedValue)(nil)).Elem()
)

// T, U, V, W, X, Y, Z are universal types. They play the role of generic
// type variables in UserFn signatures, but are limited to top-level positions.

type T interface{}
type U interface{}
type V interface{}
type W interface{}
type X interface{}
type Y interface{}
type Z interface{}

// EventTime is a timestamp that Beam understands as attached to an element.
type EventTime = mtime.Time

// Window represents a concrete Window.
type Window interface {
	// MaxTimestamp returns the the inclusive upper bound of timestamps for values in this window.
	MaxTimestamp() EventTime

	// Equals returns true iff the windows are identical.
	Equals(o Window) bool
}

// KV, CoGBK, WindowedValue represent composite generic types. They are not used
// directly in user code signatures, but only in FullTypes.

type KV struct{}

type CoGBK struct{}

type WindowedValue struct{}
