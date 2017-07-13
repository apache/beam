package beam

import (
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
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
func RegisterType(t reflect.Type) {
	runtime.RegisterType(t)
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

// TODO(herohde) 5/1/2017: add type aliases to the well-known types for Go 1.9.

// type T = typex.T
// type U = typex.U
// type V = typex.V
// type W = typex.W
// type X = typex.X
// type Y = typex.Y
// type Z = typex.Z

// type EventTime = typex.EventTime

// type KV = typex.KV
// type GBK = typex.GBK
// type CoGBK = typex.CoGBK
// type WindowedValue = typex.WindowededValue
