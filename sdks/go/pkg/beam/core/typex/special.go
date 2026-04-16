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
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
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
	TimersType    = reflect.TypeOf((*Timers)(nil)).Elem()
	PaneInfoType  = reflect.TypeOf((*PaneInfo)(nil)).Elem()

	KVType                 = reflect.TypeOf((*KV)(nil)).Elem()
	NullableType           = reflect.TypeOf((*Nullable)(nil)).Elem()
	CoGBKType              = reflect.TypeOf((*CoGBK)(nil)).Elem()
	WindowedValueType      = reflect.TypeOf((*WindowedValue)(nil)).Elem()
	BundleFinalizationType = reflect.TypeOf((*BundleFinalization)(nil)).Elem()
)

// T, U, V, W, X, Y, Z are universal types. They play the role of generic
// type variables in UserFn signatures, but are limited to top-level positions.

type T any
type U any
type V any
type W any
type X any
type Y any
type Z any

// EventTime is a timestamp that Beam understands as attached to an element.
type EventTime = mtime.Time

// Window represents a concrete Window.
type Window interface {
	// MaxTimestamp returns the inclusive upper bound of timestamps for values in this window.
	MaxTimestamp() EventTime

	// Equals returns true iff the windows are identical.
	Equals(o Window) bool
}

// BundleFinalization allows registering callbacks for the runner to invoke after the bundle completes and the runner
// commits the output. Parameter is accessible during DoFn StartBundle, ProcessElement, FinishBundle.
// However, if your DoFn implementation requires BundleFinalization in StartBundle or FinishBundle, it is needed in the
// ProcessElement signature, even if not invoked,
// Common use cases for BundleFinalization would be to perform work after elements in a bundle have been processed.
// See beam.ParDo for documentation on these DoFn lifecycle methods.
type BundleFinalization interface {

	// RegisterCallback registers the runner to invoke func() after the runner persists the bundle of processed elements.
	// The time.Duration configures the callback expiration, after which the runner will not invoke func().
	// Returning error communicates to the runner that bundle finalization failed and the runner may choose to attempt
	// finalization again.
	RegisterCallback(time.Duration, func() error)
}

// PaneTiming defines the pane timing in byte.
type PaneTiming byte

const (
	// PaneEarly defines early pane timing.
	PaneEarly PaneTiming = 0
	// PaneOnTime defines on-time pane timing.
	PaneOnTime PaneTiming = 1
	// PaneLate defines late pane timing.
	PaneLate PaneTiming = 2
	// PaneUnknown defines unknown pane timing.
	PaneUnknown PaneTiming = 3
)

func (t PaneTiming) String() string {
	switch t {
	case PaneEarly:
		return "early"
	case PaneOnTime:
		return "ontime"
	case PaneLate:
		return "late"
	default:
		return "unknown"
	}
}

// PaneInfo represents the output pane.
type PaneInfo struct {
	Timing                     PaneTiming
	IsFirst, IsLast            bool
	Index, NonSpeculativeIndex int64
}

// Timers is the actual type used for standard timer coder.
type Timers struct {
	Key                          []byte // elm type.
	Tag                          string
	Windows                      []byte // []typex.Window
	Clear                        bool
	FireTimestamp, HoldTimestamp mtime.Time
	Pane                         PaneInfo
}

// KV, Nullable, CoGBK, WindowedValue represent composite generic types. They are not used
// directly in user code signatures, but only in FullTypes.

type KV struct{}

type Nullable struct{}

type CoGBK struct{}

type WindowedValue struct{}

// ShardedKey wraps a user key with an opaque shard identifier, allowing a
// single logical key's processing to be spread across workers by a runner.
//
// A ShardedKey is a concrete Go value — unlike the empty-marker Composite
// types above (KV, CoGBK, WindowedValue), each instantiation is a distinct
// sliceable/serializable struct whose coder is the standard cross-SDK
// coder with URN "beam:coder:sharded_key:v1" (byte-compatible with the
// Java util.ShardedKey and Python sharded_key encodings).
//
// Users generally do not build ShardedKey values directly; they are
// produced by GroupIntoBatchesWithShardedKey.
type ShardedKey[K any] struct {
	// Key is the logical user key being sharded.
	Key K

	// ShardID is an opaque byte-identifier that distinguishes shards of the
	// same logical key. The wire format treats it as a raw byte string
	// (length-prefixed).
	ShardID []byte
}

// IsShardedKey reports whether t is an instantiation of ShardedKey[K] defined
// in this package.
//
// This function is the canonical way to detect a ShardedKey type across the
// SDK. Go reflection does not expose a "generic origin" of a parameterized
// type, so detection is name-based — matching types whose reflected name
// starts with "ShardedKey[" and whose package path matches this package.
func IsShardedKey(t reflect.Type) bool {
	if t == nil || t.Kind() != reflect.Struct {
		return false
	}
	if t.PkgPath() != shardedKeyPkgPath {
		return false
	}
	return strings.HasPrefix(t.Name(), "ShardedKey[")
}

// ShardedKeyKeyType returns the K type parameter of a ShardedKey[K]
// instantiation. Returns nil if t is not a ShardedKey type.
func ShardedKeyKeyType(t reflect.Type) reflect.Type {
	if !IsShardedKey(t) {
		return nil
	}
	// Fields are Key (index 0), ShardID (index 1) — see struct definition.
	return t.Field(0).Type
}

// shardedKeyInstantiations is a registry of ShardedKey[K] concrete types
// keyed by K. Registration happens at package init time via
// RegisterShardedKeyType and allows the coder unmarshaller to find the
// exact typex.ShardedKey[K] reflect.Type for a given K reflect.Type,
// without which IsShardedKey would reject the reconstructed struct.
var shardedKeyInstantiations = make(map[reflect.Type]reflect.Type)

// RegisterShardedKeyType registers the concrete reflect.Type of
// typex.ShardedKey[K] for a given key reflect.Type. This enables the
// coder unmarshaller — which only sees the wire key coder — to retrieve
// the exact ShardedKey[K] Go type. Called automatically by
// GroupIntoBatchesWithShardedKey for each K used by user code.
//
// Idempotent: calling with a previously registered keyType is a no-op.
func RegisterShardedKeyType(keyType, shardedKeyType reflect.Type) {
	if keyType == nil || shardedKeyType == nil {
		panic("RegisterShardedKeyType: keyType and shardedKeyType must be non-nil")
	}
	if !IsShardedKey(shardedKeyType) {
		panic(fmt.Sprintf(
			"RegisterShardedKeyType: %v is not a typex.ShardedKey instantiation",
			shardedKeyType))
	}
	if ShardedKeyKeyType(shardedKeyType) != keyType {
		panic(fmt.Sprintf(
			"RegisterShardedKeyType: key type mismatch — struct Key field is %v but keyType is %v",
			ShardedKeyKeyType(shardedKeyType), keyType))
	}
	shardedKeyInstantiations[keyType] = shardedKeyType
}

// LookupShardedKeyType returns the registered typex.ShardedKey[K] concrete
// reflect.Type for the given K reflect.Type, or nil if not registered.
func LookupShardedKeyType(keyType reflect.Type) reflect.Type {
	return shardedKeyInstantiations[keyType]
}

// shardedKeyPkgPath captures this package's path at init time for
// IsShardedKey's PkgPath comparison. Recorded once to avoid allocating
// a reflect.Type value per check.
var shardedKeyPkgPath = reflect.TypeOf(ShardedKey[int]{}).PkgPath()
