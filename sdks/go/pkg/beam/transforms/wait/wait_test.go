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

package wait_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/wait"
)

func init() {
	register.Function1x2(waitTestKVFn)
	register.Function2x1(waitTestFormatKVFn)
}

// TestMain invokes ptest.Main so the end-to-end tests below run on the
// configured runner (Prism by default).
func TestMain(m *testing.M) {
	ptest.Main(m)
}

// expectPanic runs f and fails the test unless f panics with a message
// containing want.
func expectPanic(t *testing.T, want string, f func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic containing %q, got no panic", want)
		}
		if msg := fmt.Sprint(r); !strings.Contains(msg, want) {
			t.Fatalf("panic message %q does not contain %q", msg, want)
		}
	}()
	f()
}

func TestOn_NoSignals(t *testing.T) {
	_, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, 1, 2, 3)
	out := wait.On(s, col)
	if out != col {
		t.Errorf("wait.On with no signals returned %v, want the input PCollection %v", out, col)
	}
}

func TestOn_NoSignalsSkipsShapeChecks(t *testing.T) {
	_, s := beam.NewPipelineWithRoot()
	cogbk := beam.CoGroupByKey(s, kvCol(s, 1, 2, 3), kvCol(s, 4, 5, 6))
	if out := wait.On(s, cogbk); out != cogbk {
		t.Errorf("wait.On with no signals on a CoGBK input returned %v, want the input PCollection %v", out, cogbk)
	}
}

func TestOn_InvalidInputsPanic(t *testing.T) {
	_, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, 1, 2, 3)
	sig := beam.Create(s, "ready")

	expectPanic(t, "wait.On: invalid scope", func() {
		wait.On(beam.Scope{}, col, sig)
	})
	expectPanic(t, "wait.On: invalid input pcollection", func() {
		wait.On(s, beam.PCollection{}, sig)
	})
	expectPanic(t, "wait.On: invalid signal pcollection: index 1", func() {
		wait.On(s, col, sig, beam.PCollection{})
	})
}

func TestOn_SessionSignalPanics(t *testing.T) {
	_, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, 1, 2, 3)
	sig := beam.WindowInto(s, window.NewSessions(time.Minute), beam.Create(s, "ready"))

	expectPanic(t, "wait.On: signal pcollection must not use session windowing (side inputs cannot map session windows): index 0", func() {
		wait.On(s, col, sig)
	})
}

func TestOn_Identity(t *testing.T) {
	ptest.BuildAndRun(t, func(s beam.Scope) {
		col := beam.Create(s, 1, 2, 3)
		sig := beam.Create(s, "ready")
		out := wait.On(s, col, sig)
		passert.Equals(s, out, 1, 2, 3)
	})
}

func TestOn_MultipleSignals(t *testing.T) {
	ptest.BuildAndRun(t, func(s beam.Scope) {
		col := beam.Create(s, 1, 2, 3)
		sigA := beam.Create(s, "a")
		sigB := beam.Create(s, 1.5)
		passert.Equals(s, wait.On(s, col, sigA, sigB), 1, 2, 3)
	})
}

func TestOn_GlobalMainNonGlobalSignalPanics(t *testing.T) {
	_, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, 1, 2, 3)
	global := beam.Create(s, "ready")
	fixed := beam.WindowInto(s, window.NewFixedWindows(time.Minute), beam.Create(s, "ready"))

	// A global signal is fine in any position; the fixed-windowed one is not.
	expectPanic(t, "wait.On: signal pcollection must be globally windowed when the input pcollection is (a global main window cannot be mapped to a non-global side-input window): index 1", func() {
		wait.On(s, col, global, fixed)
	})
	// The other direction is allowed: a windowed main input may wait on a global signal.
	windowed := beam.WindowInto(s, window.NewFixedWindows(time.Minute), col)
	if out := wait.On(s, windowed, global); !out.IsValid() {
		t.Errorf("wait.On(windowed main, global signal) returned an invalid PCollection")
	}
}

// waitTestKVFn turns an int into a KV<int, string> entry so tests can build KV
// PCollections from a package-level (non-closure) DoFn.
func waitTestKVFn(v int) (int, string) {
	return v, fmt.Sprintf("v%d", v)
}

// waitTestFormatKVFn renders a KV<int, string> as one string. passert rejects
// composite element types, so KV outputs are compared in this projected form.
func waitTestFormatKVFn(k int, v string) string {
	return fmt.Sprintf("%d:%s", k, v)
}

// kvCol returns a KV<int, string> PCollection with one entry per value.
func kvCol(s beam.Scope, values ...int) beam.PCollection {
	return beam.ParDo(s, waitTestKVFn, beam.CreateList(s, values))
}

func TestOn_KVMainInput(t *testing.T) {
	t.Run("PlainSignal", func(t *testing.T) {
		ptest.BuildAndRun(t, func(s beam.Scope) {
			main := kvCol(s, 1, 2, 3)
			sig := beam.Create(s, "ready")
			out := wait.On(s, main, sig)
			passert.Equals(s, beam.ParDo(s, waitTestFormatKVFn, out), "1:v1", "2:v2", "3:v3")
		})
	})
	t.Run("KVSignal", func(t *testing.T) {
		ptest.BuildAndRun(t, func(s beam.Scope) {
			main := kvCol(s, 1, 2, 3)
			sig := kvCol(s, 10, 20)
			out := wait.On(s, main, sig)
			passert.Equals(s, beam.ParDo(s, waitTestFormatKVFn, out), "1:v1", "2:v2", "3:v3")
		})
	})
}

func TestOn_KVSignal(t *testing.T) {
	ptest.BuildAndRun(t, func(s beam.Scope) {
		main := beam.Create(s, 1, 2, 3)
		sig := kvCol(s, 10, 20)
		out := wait.On(s, main, sig)
		passert.Equals(s, out, 1, 2, 3)
	})
}

func TestOn_CoGBKPanics(t *testing.T) {
	_, s := beam.NewPipelineWithRoot()
	cogbk := beam.CoGroupByKey(s, kvCol(s, 1, 2, 3), kvCol(s, 4, 5, 6))
	col := beam.Create(s, 1, 2, 3)
	sig := beam.Create(s, "ready")

	expectPanic(t, "wait.On: input pcollection must not be a CoGBK: ", func() {
		wait.On(s, cogbk, sig)
	})
	expectPanic(t, "wait.On: signal pcollection must not be a CoGBK: index 0: ", func() {
		wait.On(s, col, cogbk)
	})
}

// TestOn_PreservesCoder checks that the output retains the coder assigned to
// the input through every chained stage.
func TestOn_PreservesCoder(t *testing.T) {
	t.Run("Plain", func(t *testing.T) {
		_, s := beam.NewPipelineWithRoot()
		col := beam.Create(s, 1, 2, 3)
		replacement := beam.NewCoder(col.Type())
		if err := col.SetCoder(replacement); err != nil {
			t.Fatalf("SetCoder failed: %v", err)
		}
		out := wait.On(s, col, beam.Create(s, "a"), beam.Create(s, 1.5))
		if out.Coder() != replacement {
			t.Errorf("output coder %v is not the input's coder %v", out.Coder(), replacement)
		}
	})
	t.Run("KV", func(t *testing.T) {
		_, s := beam.NewPipelineWithRoot()
		col := kvCol(s, 1, 2, 3)
		replacement := beam.NewCoder(col.Type())
		if err := col.SetCoder(replacement); err != nil {
			t.Fatalf("SetCoder failed: %v", err)
		}
		out := wait.On(s, col, beam.Create(s, "a"))
		if out.Coder() != replacement {
			t.Errorf("output coder %v is not the input's coder %v", out.Coder(), replacement)
		}
	})
}
