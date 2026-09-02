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

// Package wait delays a PCollection until one or more signal PCollections are
// ready. It is the Go equivalent of Java's Wait.on.
//
// On preserves the main input but holds each of its windows until the mapped
// window in every signal has closed. Global signals map to the global window;
// fixed and sliding signals map to the earliest window containing the main
// window's maximum timestamp. A signal window closes after its watermark
// passes the window end plus allowed lateness.
//
// For example, to finish writing each window to one database before writing it
// to another:
//
//	firstWriteResults := beam.ParDo(s, writeToFirstDB, data)
//	delayed := wait.On(s, data, firstWriteResults)
//	beam.ParDo0(s, writeToSecondDB, delayed)
//
// A bounded global signal holds every main window until the entire signal is
// complete. An unbounded global signal never becomes ready. Large allowed
// lateness values delay readiness by the same amount.
//
// Go side-input window restrictions apply. Signal PCollections cannot use
// session windows, and a global main input cannot wait on a non-global signal.
// Ordinary and KV PCollections are supported. With no signals, On returns any
// valid input directly; otherwise, CoGBK PCollections are not supported.
package wait

import (
	"fmt"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window/trigger"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn3x0[typex.Window, beam.T, func(int)](&collectWindowsFn{})
	register.Function2x1(keepOneFn)
	register.Function2x1(passThroughFn)
	register.Function3x2(passThroughKVFn)
	register.Emitter1[int]()
	register.Iter1[int]()
}

// On returns col unchanged — same elements, type, coder and windowing — but
// delays each main window until its mapped window has closed in every signal.
//
// With no signals, On validates s and col and returns col directly. Otherwise,
// it panics at pipeline construction time for an invalid scope or PCollection,
// CoGBK inputs, session-windowed signals, or a non-global signal on a globally
// windowed main input.
func On(s beam.Scope, col beam.PCollection, signals ...beam.PCollection) beam.PCollection {
	if !s.IsValid() {
		panic("wait.On: invalid scope")
	}
	if !col.IsValid() {
		panic("wait.On: invalid input pcollection")
	}
	if len(signals) == 0 {
		return col
	}
	if typex.IsCoGBK(col.Type()) {
		panic(fmt.Sprintf("wait.On: input pcollection must not be a CoGBK: %v", col))
	}
	mainIsGlobal := col.WindowingStrategy().Fn.Kind == window.GlobalWindows
	for i, sig := range signals {
		if !sig.IsValid() {
			panic(fmt.Sprintf("wait.On: invalid signal pcollection: index %d", i))
		}
		if typex.IsCoGBK(sig.Type()) {
			panic(fmt.Sprintf("wait.On: signal pcollection must not be a CoGBK: index %d: %v", i, sig))
		}
		if sig.WindowingStrategy().Fn.Kind == window.Sessions {
			panic(fmt.Sprintf("wait.On: signal pcollection must not use session windowing (side inputs cannot map session windows): index %d: %v", i, sig))
		}
		if mainIsGlobal && sig.WindowingStrategy().Fn.Kind != window.GlobalWindows {
			panic(fmt.Sprintf("wait.On: signal pcollection must be globally windowed when the input pcollection is (a global main window cannot be mapped to a non-global side-input window): index %d: %v", i, sig))
		}
	}
	s = s.Scope("wait.On")
	out := col
	for i, sig := range signals {
		out = onOne(s.Scope(fmt.Sprintf("Signal(%d)", i)), out, sig)
	}
	return out
}

// onOne delays col on a single signal.
//
// The Never trigger delays each nonempty signal window's marker until
// expiration. The marker is used as a side input to an identity ParDo; its
// value is not read, but its readiness gates the main input. Empty signal
// windows become ready when the signal watermark passes them.
//
// Marker generation emits at most one marker per signal window in each bundle;
// the Combine collapses markers across bundles.
func onOne(s beam.Scope, col, signal beam.PCollection) beam.PCollection {
	ws := signal.WindowingStrategy()
	if typex.IsKV(signal.Type()) {
		signal = beam.DropKey(s, signal)
	}
	closed := beam.WindowInto(s, ws.Fn, signal,
		beam.Trigger(trigger.Never()),
		beam.PanesDiscard(),
		beam.AllowedLateness(time.Duration(ws.AllowedLateness)*time.Millisecond),
	)
	markers := beam.ParDo(s, &collectWindowsFn{}, closed)
	marker := beam.Combine(s, keepOneFn, markers)
	var out beam.PCollection
	if typex.IsKV(col.Type()) {
		out = beam.ParDo(s, passThroughKVFn, col, beam.SideInput{Input: marker})
	} else {
		out = beam.ParDo(s, passThroughFn, col, beam.SideInput{Input: marker})
	}
	// ParDo infers a fresh coder for its output. The output is col verbatim, so
	// keep col's coder — as Flatten does — rather than replace one the user set.
	if err := out.SetCoder(col.Coder()); err != nil {
		panic(fmt.Sprintf("wait.On: cannot preserve the input coder on the output: %v", err))
	}
	return out
}

// collectWindowsFn emits one marker per signal window in each bundle. Observing
// the window makes the harness process multi-window elements once per window;
// ProcessElement emitters preserve that window on their output.
type collectWindowsFn struct {
	seen map[typex.Window]struct{}
}

func (fn *collectWindowsFn) StartBundle(_ func(int)) {
	fn.seen = make(map[typex.Window]struct{})
}

func (fn *collectWindowsFn) ProcessElement(w typex.Window, _ beam.T, emit func(int)) {
	if _, ok := fn.seen[w]; ok {
		return
	}
	fn.seen[w] = struct{}{}
	emit(1)
}

// keepOneFn collapses markers to one per window.
func keepOneFn(_, _ int) int {
	return 1
}

// passThroughFn returns its input after the marker side input is ready.
func passThroughFn(elm beam.T, _ func(*int) bool) beam.T {
	return elm
}

// passThroughKVFn is passThroughFn for a KV main input.
func passThroughKVFn(k beam.X, v beam.Y, _ func(*int) bool) (beam.X, beam.Y) {
	return k, v
}
