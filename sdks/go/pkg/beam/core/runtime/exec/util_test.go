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

package exec

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/go/pkg/beam/util/errorx"
)

// testSimpleError tests for a simple case that
// doesn't panic
func testSimpleError(ctx context.Context, t *testing.T) {
	expected := errors.New("Simple error.")
	actual := callNoPanic(ctx, func(c context.Context) error { return errors.New("Simple error.") })

	if actual.Error() != expected.Error() {
		t.Errorf("Simple error reporting failed.")
	}
}

// testPanicError tests for the case in which a normal
// error is passed to panic, resulting in panic trace.
func testPanicError(ctx context.Context, t *testing.T) {
	actual := callNoPanic(ctx, func(c context.Context) error { panic("Panic error") })
	if !strings.Contains(actual.Error(), "panic:") {
		t.Errorf("Caught in panic.")
	}
}

// testWrapPanicError tests for the case in which error
// is passed to panic from DoFn, resulting in
// formatted error message for DoFn.
func testWrapPanicError(ctx context.Context, t *testing.T) {
	parDoError := doFnError{
		doFn: "sumFn",
		err:  errors.New("SumFn error"),
		uid:  1,
		pid:  "Plan ID",
	}
	var err errorx.GuardedError
	err.TrySetError(&parDoError)
	actual := callNoPanic(ctx, func(c context.Context) error { panic(parDoError) })

	if strings.Contains(actual.Error(), "panic:") {
		t.Errorf("Error not wrapped! Caught in panic")
	}
}

func TestCallNoPanic(t *testing.T) {
	ctx := context.Background()
	testSimpleError(ctx, t)
	testPanicError(ctx, t)
	testWrapPanicError(ctx, t)
}
