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

// wasm is an EXPERIMENTAL simple example that loads and executes a wasm file function.
// greet.wasm, Cargo.toml and greet.rs were copied from the example provided by the wazero library:
// https://github.com/tetratelabs/wazero/blob/v1.0.0-pre.3/examples/allocation/rust/greet.go
//
// New Concepts:
// 1. Load a wasm file compiled cargo build --release --target wasm32-unknown-unknown
// 2. Execute a wasm function within a DoFn
package main

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"log"
)

const (
	wasmFunctionName           = "greeting"
	wasmAllocateFunctionName   = "allocate"
	wasmDeallocateFunctionName = "deallocate"
)

//go:embed greet.wasm
var greetWasm []byte

var (
	output = flag.String("output", "", "Output file (required).")
)

func init() {
	// register.DoFnXxY registers a struct DoFn so that it can be correctly
	// serialized and does some optimization to avoid runtime reflection. Since
	// embeddedWasmFn's ProcessElement func has 2 inputs (context.Context) and 2 outputs (string, error),
	// we use register.DoFn2x2 and provide its input and output types as its constraints.
	// Struct DoFns must be registered for a pipeline to run.
	register.DoFn2x2[context.Context, string, string, error](&embeddedWasmFn{})
}

func preRun() error {
	if *output == "" {
		return fmt.Errorf("--output is required")
	}
	return nil
}

func main() {
	flag.Parse()
	beam.Init()
	ctx := context.Background()
	if err := preRun(); err != nil {
		panic(err)
	}
	if err := run(ctx); err != nil {
		panic(err)
	}
}

func run(ctx context.Context) error {
	p, s := beam.NewPipelineWithRoot()

	in := beam.Create(s, "a", "b", "c", "d", "e", "f", "g", "h")

	out := beam.ParDo(s.Scope("embeddedWasm"), &embeddedWasmFn{}, in)

	textio.Write(s, *output, out)

	if err := beamx.Run(ctx, p); err != nil {
		return fmt.Errorf("failed to run pipeline: %v", err)
	}
	return nil
}

// Concept #2 wrap wasm function execution within a DoFn.
// wasmFn wraps a DoFn to execute a wasmer.io compiled wasm function
type embeddedWasmFn struct {
	r                              wazero.Runtime
	mod                            api.Module
	greeting, allocate, deallocate api.Function
}

// Setup loads and initializes the embedded wasm functions
// Concept #1: Load a compiled wasm file []byte content and function.
// This example is derived from
// https://github.com/tetratelabs/wazero/blob/v1.0.0-pre.3/examples/allocation/rust/greet.go
func (fn *embeddedWasmFn) Setup(ctx context.Context) error {
	fn.r = wazero.NewRuntime(ctx)
	_, err := fn.r.NewHostModuleBuilder("env").
		NewFunctionBuilder().WithFunc(logString).Export("log").
		Instantiate(ctx, fn.r)
	if err != nil {
		return fmt.Errorf("failed to instantiate host module: %w", err)
	}
	fn.mod, err = fn.r.InstantiateModuleFromBinary(ctx, greetWasm)
	if err != nil {
		return fmt.Errorf("failed to instantiate wasm module: %v", err)
	}
	fn.greeting = fn.mod.ExportedFunction(wasmFunctionName)
	fn.allocate = fn.mod.ExportedFunction(wasmAllocateFunctionName)
	fn.deallocate = fn.mod.ExportedFunction(wasmDeallocateFunctionName)
	return nil
}

// ProcessElement processes a string calling a wasm function written in Rust
// This example is derived from
// https://github.com/tetratelabs/wazero/blob/v1.0.0-pre.3/examples/allocation/rust/greet.go
func (fn *embeddedWasmFn) ProcessElement(ctx context.Context, s string) (string, error) {
	size := uint64(len(s))
	results, err := fn.allocate.Call(ctx, size)
	if err != nil {
		return "", fmt.Errorf("error calling allocate: %w", err)
	}
	ptr := results[0]
	defer fn.deallocate.Call(ctx, ptr, size)
	if !fn.mod.Memory().Write(ctx, uint32(ptr), []byte(s)) {
		return "", fmt.Errorf("Memory.Write(%d, %d) out of range of memory size %d",
			ptr, size, fn.mod.Memory().Size(ctx))
	}

	ptrSize, err := fn.greeting.Call(ctx, ptr, size)
	resultPtr := uint32(ptrSize[0] >> 32)
	resultSize := uint32(ptrSize[0])
	defer fn.deallocate.Call(ctx, uint64(resultPtr), uint64(resultSize))
	bytes, ok := fn.mod.Memory().Read(ctx, resultPtr, resultSize)
	if !ok {
		return "", fmt.Errorf("Memory.Read(%d, %d) out of range of memory size %d",
			resultPtr, resultSize, fn.mod.Memory().Size(ctx))
	}
	return string(bytes), nil
}

func logString(ctx context.Context, m api.Module, offset, byteCount uint32) {
	buf, ok := m.Memory().Read(ctx, offset, byteCount)
	if !ok {
		log.Panicf("Memory.Read(%d, %d) out of range", offset, byteCount)
	}
	fmt.Println(string(buf))
}
