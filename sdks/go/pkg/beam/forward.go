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

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

// IMPLEMENTATION NOTE: functions and types in this file are assumed to be
// simple forwards from the graph package (or subpackages) for the purpose of
// removing such package dependencies from end-user pipeline code. So:
//
//         PLEASE DO NOT ADD NON-FORWARDING FUNCTIONALITY HERE.
//
// Instead, add such functionality in the core packages.

// TODO(herohde) 7/13/2017: these forwards alone pull in runtime. Is there a use
// case for separate package?

// RegisterType inserts "external" types into a global type registry to bypass
// serialization and preserve full method information. It should be called in
// init() only.
// TODO(wcn): the canonical definition of "external" is in v1.proto. We need user
// facing copy for this important concept.
func RegisterType(t reflect.Type) {
	runtime.RegisterType(t)
}

// RegisterFunction allows function registration. It is beneficial for performance
// and is needed for functions -- such as custom coders -- serialized during unit
// tests, where the underlying symbol table is not available. It should be called
// in init() only. Returns the external key for the function.
func RegisterFunction(fn interface{}) {
	runtime.RegisterFunction(fn)
}

// RegisterInit registers an Init hook. Hooks are expected to be able to
// figure out whether they apply on their own, notably if invoked in a remote
// execution environment. They are all executed regardless of the runner.
func RegisterInit(hook func()) {
	runtime.RegisterInit(hook)
}

// Init is the hook that all user code must call after flags processing and
// other static initialization, for now.
func Init() {
	runtime.Init()
}

// PipelineOptions are global options for the active pipeline. Options can
// be defined any time before execution and are re-created by the harness on
// remote execution workers. Global options should be used sparingly.
var PipelineOptions = runtime.GlobalOptions

// We forward typex types used in UserFn signatures to avoid having such code
// depend on the typex package directly.

type FullType = typex.FullType

type T = typex.T
type U = typex.U
type V = typex.V
type W = typex.W
type X = typex.X
type Y = typex.Y
type Z = typex.Z

type EventTime = typex.EventTime

var TType = typex.TType
var UType = typex.UType
var VType = typex.VType
var WType = typex.WType
var XType = typex.XType
var YType = typex.YType
var ZType = typex.ZType

var EventTimeType = typex.EventTimeType
