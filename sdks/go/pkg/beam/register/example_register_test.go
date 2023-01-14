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

package register_test

import (
	"context"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

type myDoFn struct{}

func (fn *myDoFn) ProcessElement(word string, iter func(*string) bool, emit func(int)) int {
	var s string
	for iter(&s) {
		emit(len(s))
	}
	return len(word)
}

func (fn *myDoFn) StartBundle(_ context.Context, emit func(int)) {
	emit(2)
}

func (fn *myDoFn) FinishBundle(_ context.Context, emit func(int)) error {
	emit(2)
	return nil
}

func (fn *myDoFn) Setup(_ context.Context) error {
	return nil
}

func (fn *myDoFn) Teardown() error {
	return nil
}

type myDoFn2 struct{}

type Foo struct {
	s string
}

func (fn *myDoFn2) ProcessElement(word string, iter func(**Foo, *beam.EventTime) bool, emit func(beam.EventTime, string, int)) (beam.EventTime, string, int) {
	var f *Foo
	var et beam.EventTime
	for iter(&f, &et) {
		emit(et, f.s, len(f.s))
	}
	return mtime.Now(), word, len(word)
}

func ExampleDoFn3x1() {
	// Since myDoFn's ProcessElement call has 3 inputs and 1 output, call DoFn3x1.
	// Since the inputs to ProcessElement are (string, func(int)), and the output
	// is int, we pass those parameter types to the function.
	register.DoFn3x1[string, func(*string) bool, func(int), int](&myDoFn{})

	// Any function parameters (iters or emitters) must be registered separately
	// as well to get the fully optimized experience. Since ProcessElement has
	// an emitter with the signature func(int) we can register it. This must be
	// done by passing in the type parameters of all inputs as constraints.
	register.Emitter1[int]()
	register.Iter1[string]()

	register.DoFn3x3[string, func(**Foo, *beam.EventTime) bool, func(beam.EventTime, string, int), beam.EventTime, string, int](&myDoFn2{})

	// More complex iter/emitter registration work in the same way, even when
	// timestamps or pointers are involved.
	register.Emitter3[beam.EventTime, string, int]()
	register.Iter2[*Foo, beam.EventTime]()
}
