package beam

import (
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
)

// IMPLEMENTATION NOTE: functions and types in this file are assumed to be
// simple forwards from the graph package (or subpackages) for the purpose of
// removing such package dependencies from end-user pipeline code. So:
//
//         PLEASE DO NOT ADD NON-FORWARDING FUNCTIONALITY HERE.
//
// Instead, add such functionality in the graph packages.

// RegisterType inserts "external" structs into a global type registry to bypass
// serialization and preserve full method information. It should be called in
// init() only.
func RegisterType(t reflect.Type) {
	graph.Register(t)
}

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
