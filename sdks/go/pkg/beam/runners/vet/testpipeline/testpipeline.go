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

// Package testpipeline exports small test pipelines for testing the vet
// runner. Shims must be generated for this package in order for tests to run
// correctly. These shims should be regenerated if changes are made to this
// package or to the shim generator.
package testpipeline

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

// Performant constructs a performant pipeline.
func Performant(s beam.Scope) {
	vs := beam.Create(s, 1, 2, 3)
	kvs := beam.ParDo(s, VFn, vs)
	kv1 := beam.ParDo(s, KvFn, kvs)
	kv2 := beam.ParDo(s, KvEmitFn, kvs)
	flatKvs := beam.Flatten(s, kv1, kv2)

	beam.CombinePerKey(s, &SCombine{}, flatKvs)
}

// FunctionReg constructs a sub optimal pipeline that needs function registration.
func FunctionReg(s beam.Scope) {
	vs := beam.Create(s, float64(1), float64(2), float64(3))
	kvs := beam.ParDo(s, VFloat64Fn, vs)
	beam.CombinePerKey(s, &SCombine{}, kvs)
}

// ShimNeeded constructs a sub optimal pipeline that needs a function shim registration.
func ShimNeeded(s beam.Scope) {
	vs := beam.Create(s, float64(1), float64(2), float64(3))
	kvs := beam.ParDo(s, vFloat64Fn, vs)
	beam.CombinePerKey(s, &SCombine{}, kvs)
}

// TypeReg constructs a sub optimal pipeline that needs type registration.
func TypeReg(s beam.Scope) {
	vs := beam.Create(s, 1, 2, 3)
	kvs := beam.ParDo(s, VFn, vs)

	c := beam.CombinePerKey(s, &SCombine{}, kvs)
	beam.ParDo(s, toFooFn, c)
}

// VFloat64Fn is an unregistered function without type shims.
func VFloat64Fn(v float64) (string, int) {
	return "key", 0
}

func init() {
	beam.RegisterFunction(vFloat64Fn)
}

// vFloat64Fn is a registered function without type shims.
func vFloat64Fn(v float64) (string, int) {
	return "key", 0
}

// foo is an unregistered, unexported user type.
type foo struct {
	K string
	V int
}

// toFooFn is an unregistered function, that uses an unregistered user type,
// without a shim.
func toFooFn(k string, v int) foo {
	return foo{K: k, V: v}
}
