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

package registration_test

import (
	"context"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/registration"
)

type myDoFn struct {
}

func (fn *myDoFn) ProcessElement(word string, emit func(int)) int {
	emit(len(word))
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

func ExampleDoFn2x1() {
	// Since myDoFn's ProcessElement call has 2 inputs and 1 output, call DoFn2x1.
	// Since the inputs to ProcessElement are (string, func(int)), and the output
	// is int, we pass those parameter types to the function.
	registration.DoFn2x1[string, func(int), int](&myDoFn{})
}
