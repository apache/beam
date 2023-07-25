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

// wasm is a simple example that loads and executes a wasm file function.
// greet.wasm, Cargo.toml and greet.rs were copied from the example provided by the wazero library:
// https://github.com/tetratelabs/wazero/blob/v1.0.0-pre.3/examples/allocation/rust/greet.go
//
// New Concepts:
// 1. Load a wasm file compiled from: cargo build --release --target wasm32-unknown-unknown
// 2. Execute a wasm function within a DoFn
package main

import (
	"context"
	_ "embed"
	"flag"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
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
		log.Fatal(ctx, err)
	}
	if err := run(ctx); err != nil {
		log.Fatal(ctx, err)
	}
}

func run(ctx context.Context) error {
	p, s := beam.NewPipelineWithRoot()

	in := beam.Create(s, "Ada", "Lovelace", "World", "Beam", "Senior López", "Random unicorn emoji 🦄")

	out := beam.ParDo(s, &embeddedWasmFn{}, in)

	textio.Write(s, *output, out)

	if err := beamx.Run(ctx, p); err != nil {
		return fmt.Errorf("failed to run pipeline: %v", err)
	}
	return nil
}

// Concept #2 wrap wasm function execution within a DoFn.
// wasmFn wraps a DoFn to execute a Rust compiled wasm function
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
	// Create a new WebAssembly Runtime.
	// Typically, a defer r.Close() would be called subsequently after.  Yet, we need to keep this in memory
	// throughout the DoFn lifecycle after which we invoke r.Close(); see Teardown below.
	fn.r = wazero.NewRuntime(ctx)

	// Instantiate a Go-defined module named "env" that exports a function to
	// log to the console.
	_, err := fn.r.NewHostModuleBuilder("env").
		NewFunctionBuilder().WithFunc(logString).Export("log").
		Instantiate(ctx)
	if err != nil {
		return fmt.Errorf("failed to instantiate host module: %w", err)
	}

	// Instantiate a WebAssembly module that imports the "log" function defined
	// in "env" and exports "memory" and functions we'll use in this example.
	fn.mod, err = fn.r.Instantiate(ctx, greetWasm)
	if err != nil {
		return fmt.Errorf("failed to instantiate wasm module: %v", err)
	}

	// Get references to WebAssembly functions we'll use in this example.
	fn.greeting = fn.mod.ExportedFunction(wasmFunctionName)
	fn.allocate = fn.mod.ExportedFunction(wasmAllocateFunctionName)
	fn.deallocate = fn.mod.ExportedFunction(wasmDeallocateFunctionName)
	return nil
}

// ProcessElement processes a string calling a wasm function written in Rust
// This example is derived from
// https://github.com/tetratelabs/wazero/blob/v1.0.0-pre.3/examples/allocation/rust/greet.go
func (fn *embeddedWasmFn) ProcessElement(ctx context.Context, s string) (string, error) {

	// We need to compute the size of s to use Rust's memory allocator.
	size := uint64(len(s))

	// Instead of an arbitrary memory offset, use Rust's allocator. Notice
	// there is nothing string-specific in this allocation function. The same
	// function could be used to pass binary serialized data to Wasm.
	results, err := fn.allocate.Call(ctx, size)
	if err != nil {
		return "", fmt.Errorf("error calling allocate: %w", err)
	}
	ptr := results[0]

	// This pointer was allocated by Rust, but owned by Go, So, we have to
	// deallocate it when finished; defer means that this statement will be called when the function exits
	defer fn.deallocate.Call(ctx, ptr, size)

	// The pointer is a linear memory offset, which is where we write the value of the DoFn's input element s.
	if !fn.mod.Memory().Write(uint32(ptr), []byte(s)) {
		return "", fmt.Errorf("Memory.Write(%d, %d) out of range of memory size %d",
			ptr, size, fn.mod.Memory().Size())
	}

	// Finally, we get the greeting message "Hello" concatenated to the DoFn's input element s.
	// This shows how to read-back something allocated by Rust.
	ptrSize, err := fn.greeting.Call(ctx, ptr, size)
	resultPtr := uint32(ptrSize[0] >> 32)
	resultSize := uint32(ptrSize[0])

	// This pointer was allocated by Rust, but owned by Go, So, we have to
	// deallocate it when finished; again defer flags Go to execute this statement when the function exits
	defer fn.deallocate.Call(ctx, uint64(resultPtr), uint64(resultSize))

	// The pointer is a linear memory offset, which is where we wrote the results of the string concatenation.
	bytes, ok := fn.mod.Memory().Read(resultPtr, resultSize)
	if !ok {
		return "", fmt.Errorf("Memory.Read(%d, %d) out of range of memory size %d",
			resultPtr, resultSize, fn.mod.Memory().Size())
	}

	// bytes contains our final result that we emit into the output PCollection
	return string(bytes), nil
}

// Teardown the wazero.Runtime during the DoFn teardown lifecycle
func (fn *embeddedWasmFn) Teardown(ctx context.Context) error {
	// Typically we would proceed wazero.Runtime's Close method with Go's defer keyword, just after instantiation.
	// However, we need to keep the property in memory until the end of the DoFn lifecycle
	if err := fn.r.Close(ctx); err != nil {
		return fmt.Errorf("failed to close runtime: %w", err)
	}
	return nil
}

// logString is an exported function to the wasm module that logs to console output.
func logString(ctx context.Context, m api.Module, offset, byteCount uint32) {
	buf, ok := m.Memory().Read(offset, byteCount)
	if !ok {
		log.Fatalf(ctx, "Memory.Read(%d, %d) out of range", offset, byteCount)
	}
	log.Info(ctx, string(buf))
}
