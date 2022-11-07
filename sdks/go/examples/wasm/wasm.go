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
// simple.wasm and simple.rs were copied from the example provided by the wasmer-go library:
// https://github.com/wasmerio/wasmer-go/tree/master/examples/appendices
//
// New Concepts:
// 1. Load a wasm file compiled using https://wasmer.io/
// 2. Execute a wasm function within a DoFn
package main

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
	"github.com/wasmerio/wasmer-go/wasmer"
	"io"
)

const (
	wasmFunctionName = "sum"
)

//go:embed simple.wasm
var simple []byte

func init() {
	// register.DoFnXxY registers a struct DoFn so that it can be correctly
	// serialized and does some optimization to avoid runtime reflection. Since
	// wasmFn's ProcessElement func has 1 input (elem) and 2 outputs (int32, error),
	// we use register.DoFn1x2 and provide its input and output types as its constraints.
	// Struct DoFns must be registered for a pipeline to run.
	register.DoFn1x2[elem, int32, error](&wasmFn{})
	// register.FunctionXxY registers a functional DoFn to optimize execution at runtime.
	// vToElemFn takes 1 input (int32) and returns 1 output (elem).
	register.Function1x1(vToElemFn)
}

func main() {
	if err := run(context.Background()); err != nil {
		panic(err)
	}
}

func run(ctx context.Context) error {
	fn, err := loadWasmFunction(bytes.NewReader(simple), wasmFunctionName)
	if err != nil {
		return fmt.Errorf("could not load wasm function")
	}

	p, s := beam.NewPipelineWithRoot()

	in := beam.CreateList(s, []int32{1, 2, 3, 4, 5, 6, 7, 8})

	elems := beam.ParDo(s.Scope("elems"), vToElemFn, in)

	result := beam.ParDo(s.Scope("sum"), &wasmFn{
		fn: fn,
	}, elems)

	debug.Printf(s, "SUM: %v\n", result)

	if err := beamx.Run(ctx, p); err != nil {
		return fmt.Errorf("failed to run pipeline: %v", err)
	}
	return nil
}

type elem struct {
	x int32
	y int32
}

// Concept #1: Load a wasmer.io compiled wasm file and function.
// This example is copied from https://pkg.go.dev/github.com/wasmerio/wasmer-go/wasmer#hdr-Examples
func loadWasmFunction(wasmFile io.Reader, wasmFnName string) (wasmer.NativeFunction, error) {
	engine := wasmer.NewEngine()
	store := wasmer.NewStore(engine)
	b, err := io.ReadAll(wasmFile)
	if err != nil {
		return nil, fmt.Errorf("could not read wasm file: %w", err)
	}
	module, err := wasmer.NewModule(store, b)
	if err != nil {
		return nil, fmt.Errorf("could not parse wasm file: %w", err)
	}
	importObj := wasmer.NewImportObject()

	instance, err := wasmer.NewInstance(module, importObj)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate wasm module: %w", err)
	}

	fn, err := instance.Exports.GetFunction(wasmFnName)
	if err != nil {
		return nil, fmt.Errorf("could not get wasm function: %w", err)
	}
	return fn, nil
}

// vToElemFn assigns an int32 value to elem.x and elem.y
func vToElemFn(v int32) elem {
	return elem{
		x: v,
		y: v,
	}
}

// wasmFn wraps a DoFn to execute a wasmer.io compiled wasm function
type wasmFn struct {
	fn wasmer.NativeFunction
}

func (fn *wasmFn) ProcessElement(e elem) (int32, error) {
	result, err := fn.fn(e.x, e.y)
	if err != nil {
		return 0, err
	}
	return result.(int32), nil
}
