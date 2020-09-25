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

package beam

import (
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/genx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

// IMPLEMENTATION NOTE: functions and types in this file are assumed to be
// simple forwards from the graph package (or subpackages) for the purpose of
// removing such package dependencies from end-user pipeline code. So:
//
//         PLEASE DO NOT ADD NON-FORWARDING FUNCTIONALITY HERE.
//
// Instead, add such functionality in the core packages and add pipeline author
// oriented documentation.

// TODO(herohde) 7/13/2017: these forwards alone pull in runtime. Is there a use
// case for separate package?

// RegisterType inserts "external" types into a global type registry to bypass
// serialization and preserve full method information. It should be called in
// `init()` only.
// TODO(wcn): the canonical definition of "external" is in v1.proto. We need user
// facing copy for this important concept.
func RegisterType(t reflect.Type) {
	runtime.RegisterType(t)
}

// RegisterFunction allows function registration. It is beneficial for performance
// and is needed for functions -- such as custom coders -- serialized during unit
// tests, where the underlying symbol table is not available. It should be called
// in `init()` only.
func RegisterFunction(fn interface{}) {
	runtime.RegisterFunction(fn)
}

// RegisterDoFn is a convenience function to handle registering a DoFn and all
// related types. Use this instead of calling RegisterType or RegisterFunction.
// Like all the Register* functions, RegisterDoFn should be called in
// `init()` only.
//
// In particular, it will call RegisterFunction for functional DoFns, and
// RegisterType for the parameter and return types for that function.
// StructuralDoFns will have RegisterType called for itself and the parameter and
// return types.
//
// RegisterDoFn will panic if the argument type is not a DoFn.
//
// Usage:
//    func init() {
//	    beam.RegisterDoFn(FunctionalDoFn)
//	    beam.RegisterDoFn(reflect.TypeOf((*StructuralDoFn)(nil)).Elem())
//    }
//
func RegisterDoFn(dofn interface{}) {
	genx.RegisterDoFn(dofn)
}

// RegisterInit registers an Init hook. Hooks are expected to be able to
// figure out whether they apply on their own, notably if invoked in a remote
// execution environment. They are all executed regardless of the runner.
func RegisterInit(hook func()) {
	runtime.RegisterInit(hook)
}

// RegisterCoder registers a user defined coder for a given type, and will
// be used if there is no existing beam coder for that type.
// Must be called prior to beam.Init(), preferably in an init() function.
//
// The coder used for a given type follows this ordering:
//   1. Coders for Known Beam types.
//   2. Coders registered for specific types
//   3. Coders registered for interfaces types
//   4. Default coder (JSON)
//
// Coders for interface types are iterated over to check if a type
// satisfies them, and the most recent one registered will be used.
//
// Repeated registrations of the same type overrides prior ones.
//
// RegisterCoder additionally registers the type, and coder functions
// as per RegisterType and RegisterFunction to avoid redundant calls.
//
// Supported Encoder Signatures
//
//  func(T) []byte
//  func(reflect.Type, T) []byte
//  func(T) ([]byte, error)
//  func(reflect.Type, T) ([]byte, error)
//
// Supported Decoder Signatures
//
//  func([]byte) T
//  func(reflect.Type, []byte) T
//  func([]byte) (T, error)
//  func(reflect.Type, []byte) (T, error)
//
// where T is the matching user type.
func RegisterCoder(t reflect.Type, encoder, decoder interface{}) {
	runtime.RegisterType(t)
	runtime.RegisterFunction(encoder)
	runtime.RegisterFunction(decoder)
	coder.RegisterCoder(t, encoder, decoder)
}

// ElementEncoder encapsulates being able to encode an element into a writer.
type ElementEncoder = coder.ElementEncoder

// ElementDecoder encapsulates being able to decode an element from a reader.
type ElementDecoder = coder.ElementDecoder

// Init is the hook that all user code must call after flags processing and
// other static initialization, for now.
func Init() {
	runtime.Init()
}

// Initialized exposes the initialization status for runners.
func Initialized() bool {
	return runtime.Initialized()
}

// PipelineOptions are global options for the active pipeline. Options can
// be defined any time before execution and are re-created by the harness on
// remote execution workers. Global options should be used sparingly.
var PipelineOptions = runtime.GlobalOptions

// We forward typex types used in UserFn signatures to avoid having such code
// depend on the typex package directly.

// FullType represents the tree structure of data types processed by the graph.
// It allows representation of composite types, such as KV<int, string> or
// CoGBK<int, int>, as well as "generic" such types, KV<int,T> or CoGBK<X,Y>,
// where the free "type variables" are the fixed universal types: T, X, etc.
type FullType = typex.FullType

// T is a Universal Type used to represent "generic" types in DoFn and
// PCollection signatures. Each universal type is distinct from all others.
type T = typex.T

// U is a Universal Type used to represent "generic" types in DoFn and
// PCollection signatures. Each universal type is distinct from all others.
type U = typex.U

// V is a Universal Type used to represent "generic" types in DoFn and
// PCollection signatures. Each universal type is distinct from all others.
type V = typex.V

// W is a Universal Type used to represent "generic" types in DoFn and
// PCollection signatures. Each universal type is distinct from all others.
type W = typex.W

// X is a Universal Type used to represent "generic" types in DoFn and
// PCollection signatures. Each universal type is distinct from all others.
type X = typex.X

// Y is a Universal Type used to represent "generic" types in DoFn and
// PCollection signatures. Each universal type is distinct from all others.
type Y = typex.Y

// Z is a Universal Type used to represent "generic" types in DoFn and
// PCollection signatures. Each universal type is distinct from all others.
type Z = typex.Z

// EventTime represents the time of the event that generated an element.
// This is distinct from the time when an element is processed.
type EventTime = typex.EventTime

// Window represents the aggregation window of this element. An element can
// be a part of multiple windows, based on the element's event time.
type Window = typex.Window

// These are the reflect.Type instances of the universal types, which are used
// when binding actual types to "generic" DoFns that use Universal Types.
var (
	TType = typex.TType
	UType = typex.UType
	VType = typex.VType
	WType = typex.WType
	XType = typex.XType
	YType = typex.YType
	ZType = typex.ZType
)

// EventTimeType is the reflect.Type of EventTime.
var EventTimeType = typex.EventTimeType
