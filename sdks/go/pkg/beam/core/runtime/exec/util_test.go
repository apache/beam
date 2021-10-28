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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/errorx"
)

// testSimpleError tests for a simple case that doesn't panic
func TestCallNoPanic_simple(t *testing.T) {
	ctx := context.Background()
	want := errors.New("Simple error.")
	got := callNoPanic(ctx, func(c context.Context) error { return errors.New("Simple error.") })

	if got.Error() != want.Error() {
		t.Errorf("callNoPanic(<func that returns error>) = %v, want %v", got, want)
	}
}

// testPanicError tests for the case in which a normal error is passed to panic, resulting in panic trace.
func TestCallNoPanic_panic(t *testing.T) {
	ctx := context.Background()
	got := callNoPanic(ctx, func(c context.Context) error { panic("Panic error") })
	if !strings.Contains(got.Error(), "panic:") {
		t.Errorf("callNoPanic(<func that panics with a string>) didn't panic, got = %v", got)
	}
}

// testWrapPanicError tests for the case in which error is passed to panic from
// DoFn, resulting in formatted error message for DoFn.
func TestCallNoPanic_wrappedPanic(t *testing.T) {
	ctx := context.Background()
	errs := errors.New("SumFn error")
	parDoError := &doFnError{
		doFn: "sumFn",
		err:  errs,
		uid:  1,
		pid:  "Plan ID",
	}
	want := "DoFn[<1>;<Plan ID>]<sumFn> returned error:<SumFn error>"
	var err errorx.GuardedError
	err.TrySetError(parDoError)

	got := callNoPanic(ctx, func(c context.Context) error { panic(parDoError) })

	if strings.Contains(got.Error(), "panic:") {
		t.Errorf("callNoPanic(<func that panics with a wrapped known error>) did not filter panic, want %v, got %v", want, got)
	}
}
