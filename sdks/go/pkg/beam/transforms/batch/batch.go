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

// Package batch provides transforms that group elements of a KV-keyed
// PCollection into batches of a target size for downstream per-batch
// processing (rate-limited API calls, bulk sinks, etc.).
//
// The primary entry point is GroupIntoBatches, which mirrors the
// behavior of the Java and Python transforms of the same name.
//
// # Behavior
//
// Given a PCollection<KV<K, V>>, GroupIntoBatches buffers values per
// key and emits batches as KV<K, []V> whenever one of the following
// limits is reached:
//
//   - len(batch) reaches BatchSize, OR
//   - sum of byte sizes reaches BatchSizeBytes, OR
//   - MaxBufferingDuration elapses in processing time since the first
//     element of the current batch (if set), OR
//   - the window advances past MaxTimestamp + AllowedLateness of the
//     input PCollection's WindowingStrategy.
//
// Elements of different windows are never combined into the same
// batch.
//
// # Determinism requirement
//
// The key coder MUST be deterministic. State keying depends on
// byte-stable encodings: a non-deterministic key coder would silently
// split the logical key across multiple physical keys, producing
// corrupt batches. The transform panics at pipeline build time if the
// key coder is not known to be deterministic. For user-defined key
// types, register the type's coder via
// coder.RegisterDeterministicCoder.
//
// # Differences from Java/Python
//
//   - Params.ElementByteSize is optional when the value type is a
//     built-in primitive ([]byte, string, numeric, bool); a fallback
//     sizer is used. For any other value type, callers must provide
//     ElementByteSize or the pipeline panics at build time.
//   - Allowed lateness is honored (matching Java). Python's transform
//     currently ignores it.
//   - BatchSize / BatchSizeBytes are int64 (parity with proto and Java
//     long, avoiding overflow on 32-bit platforms).
package batch

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/timers"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

var stringType = reflect.TypeOf("")

// Params configures GroupIntoBatches.
//
// At least one of BatchSize or BatchSizeBytes must be > 0.
type Params struct {
	// BatchSize is the target maximum number of elements per batch. A
	// batch is emitted as soon as it holds BatchSize elements. Zero
	// disables the count-based trigger.
	BatchSize int64

	// BatchSizeBytes is the target maximum cumulative byte size per
	// batch. A batch is emitted as soon as adding another element
	// would exceed BatchSizeBytes. Zero disables the byte-based
	// trigger.
	//
	// When non-zero, the value type must be one of the built-in
	// primitives ([]byte, string, numeric, bool); otherwise the
	// pipeline panics at build time.
	BatchSizeBytes int64

	// MaxBufferingDuration, when > 0, triggers emission of a partial
	// batch after this much processing time has elapsed since the
	// first element of the current batch was buffered.
	MaxBufferingDuration time.Duration
}

func (p Params) validate() error {
	if p.BatchSize < 0 {
		return fmt.Errorf("Params.BatchSize must be >= 0; got %d", p.BatchSize)
	}
	if p.BatchSizeBytes < 0 {
		return fmt.Errorf("Params.BatchSizeBytes must be >= 0; got %d", p.BatchSizeBytes)
	}
	if p.BatchSize == 0 && p.BatchSizeBytes == 0 {
		return fmt.Errorf("Params: at least one of BatchSize or BatchSizeBytes must be > 0")
	}
	if p.MaxBufferingDuration < 0 {
		return fmt.Errorf("Params.MaxBufferingDuration must be >= 0; got %s", p.MaxBufferingDuration)
	}
	return nil
}

// Sizer kind constants pick how to size values at runtime when
// BatchSizeBytes > 0.
const (
	sizerNone      int32 = 0
	sizerPrimitive int32 = 1
)

// groupIntoBatchesFn is the stateful DoFn that buffers values per key
// and emits them as batches.
//
// The DoFn fixes the key and value types to string in this release
// because Go SDK state (state.Bag[T]) requires a concrete element type —
// the SDK does not currently bind universal types through state field
// coders. Support for arbitrary types will follow once the SDK exposes
// type-parameterized state, tracked as an item in the package-level
// godoc "Future extensions" section.
type groupIntoBatchesFn struct {
	// Bag state holds the buffered values for the active batch.
	Buffer state.Bag[string]
	// Count is the current element count of the active batch.
	Count state.Value[int64]
	// ByteSize is the current cumulative byte size of the active
	// batch; unused when BatchSizeBytes == 0.
	ByteSize state.Value[int64]
	// TimerSet records whether a MaxBufferingDuration timer is armed
	// for the active batch, so flush can clear it.
	TimerSet state.Value[bool]

	// Buffering is the processing-time timer that flushes a partial
	// batch after MaxBufferingDuration elapses.
	Buffering timers.ProcessingTime
	// WindowEnd is the event-time timer that flushes the buffered
	// batch when the watermark reaches window.MaxTimestamp +
	// allowedLateness.
	WindowEnd timers.EventTime

	BatchSize         int64
	BatchSizeBytes    int64
	MaxBufferingMs    int64
	AllowedLatenessMs int64
	SizerKind         int32
}

// ProcessElement buffers value, updates state, sets timers on the
// first element of a new batch, and flushes when a size threshold is
// reached.
func (fn *groupIntoBatchesFn) ProcessElement(
	w beam.Window,
	sp state.Provider,
	tp timers.Provider,
	key string,
	value string,
	emit func(string, []string),
) {
	count, _, err := fn.Count.Read(sp)
	if err != nil {
		panic(err)
	}

	// Set the event-time timer on every element in the window to flush
	// the partial batch at end-of-window + allowed lateness. In the
	// GlobalWindow, MaxTimestamp is already at the maximum SDK value;
	// setting a timer there is a no-op for liveness (the pipeline
	// terminates by size-triggered flushes or user EOF). Skip it to
	// avoid holding the watermark.
	//
	// WithNoOutputTimestamp keeps the timer's hold from blocking the
	// output watermark — we use the timer as a flush trigger, not a
	// watermark placeholder.
	if w.MaxTimestamp() < mtime.MaxTimestamp {
		windowEnd := w.MaxTimestamp().ToTime()
		if fn.AllowedLatenessMs > 0 {
			windowEnd = windowEnd.Add(time.Duration(fn.AllowedLatenessMs) * time.Millisecond)
		}
		fn.WindowEnd.Set(tp, windowEnd, timers.WithNoOutputTimestamp())
	}

	if err := fn.Buffer.Add(sp, value); err != nil {
		panic(err)
	}
	count++
	if err := fn.Count.Write(sp, count); err != nil {
		panic(err)
	}

	newBytes := int64(0)
	if fn.BatchSizeBytes > 0 {
		cur, _, err := fn.ByteSize.Read(sp)
		if err != nil {
			panic(err)
		}
		cur += sizeOf(fn.SizerKind, value)
		if err := fn.ByteSize.Write(sp, cur); err != nil {
			panic(err)
		}
		newBytes = cur
	}

	// Set processing-time buffering timer on first element of a batch.
	if count == 1 && fn.MaxBufferingMs > 0 {
		fn.Buffering.Set(tp, time.Now().Add(time.Duration(fn.MaxBufferingMs)*time.Millisecond))
		if err := fn.TimerSet.Write(sp, true); err != nil {
			panic(err)
		}
	}

	// Flush on count or byte threshold.
	if fn.BatchSize > 0 && count >= fn.BatchSize {
		fn.flush(sp, tp, key, emit)
		return
	}
	if fn.BatchSizeBytes > 0 && newBytes >= fn.BatchSizeBytes {
		fn.flush(sp, tp, key, emit)
		return
	}
}

// OnTimer dispatches on the two timer families this DoFn registers.
// Both simply flush any pending batch.
func (fn *groupIntoBatchesFn) OnTimer(
	ctx context.Context,
	ts beam.EventTime,
	sp state.Provider,
	tp timers.Provider,
	key string,
	timer timers.Context,
	emit func(string, []string),
) {
	switch timer.Family {
	case fn.Buffering.Family, fn.WindowEnd.Family:
		fn.flush(sp, tp, key, emit)
	default:
		panic(fmt.Sprintf(
			"batch.groupIntoBatchesFn: unexpected timer family %q (tag=%q)",
			timer.Family, timer.Tag))
	}
}

// flush emits the buffered values as a single batch and clears state.
func (fn *groupIntoBatchesFn) flush(
	sp state.Provider,
	tp timers.Provider,
	key string,
	emit func(string, []string),
) {
	buf, ok, err := fn.Buffer.Read(sp)
	if err != nil {
		panic(err)
	}
	if !ok || len(buf) == 0 {
		return
	}

	emit(key, buf)

	if err := fn.Buffer.Clear(sp); err != nil {
		panic(err)
	}
	if err := fn.Count.Clear(sp); err != nil {
		panic(err)
	}
	if fn.BatchSizeBytes > 0 {
		if err := fn.ByteSize.Clear(sp); err != nil {
			panic(err)
		}
	}
	// Clear the processing-time timer so a leftover one does not fire
	// on an empty bag.
	if fn.MaxBufferingMs > 0 {
		setBool, _, err := fn.TimerSet.Read(sp)
		if err != nil {
			panic(err)
		}
		if setBool {
			fn.Buffering.Clear(tp)
			if err := fn.TimerSet.Clear(sp); err != nil {
				panic(err)
			}
		}
	}
}

// sizeOf dispatches on the Sizer kind set at graph-construction time.
func sizeOf(kind int32, v any) int64 {
	switch kind {
	case sizerNone:
		return 0
	case sizerPrimitive:
		if size, ok := defaultElementByteSize(v); ok {
			return size
		}
		panic(fmt.Sprintf("batch: sizerPrimitive cannot size value of type %T", v))
	default:
		panic(fmt.Sprintf("batch: unknown sizer kind %d", kind))
	}
}

func init() {
	register.DoFn6x0[
		beam.Window,
		state.Provider,
		timers.Provider,
		string,
		string,
		func(string, []string),
	](&groupIntoBatchesFn{})
	register.Emitter2[string, []string]()
}

// GroupIntoBatches groups the values of the input PCollection<KV<string,
// string>> into batches of up to params.BatchSize elements (or
// params.BatchSizeBytes bytes) per key and emits them as
// PCollection<KV<string, []string>>.
//
// This release supports only KV<string, string> because Go SDK state
// (state.Bag) requires a concrete element type at graph-construction
// time; once the SDK binds universal types through state field coders,
// this transform will accept arbitrary KV<K, V>.
//
// Panics at pipeline build time on invalid params, non-KV input, zero
// limits, or key coder not deterministic (string is deterministic by
// default so this check is informational only today).
func GroupIntoBatches(s beam.Scope, params Params, col beam.PCollection) beam.PCollection {
	s = s.Scope("batch.GroupIntoBatches")

	if err := params.validate(); err != nil {
		panic(fmt.Errorf("GroupIntoBatches: %w", err))
	}
	if !typex.IsKV(col.Type()) {
		panic(fmt.Errorf(
			"GroupIntoBatches: input PCollection must be KV-typed; got %v", col.Type()))
	}

	keyFT := col.Type().Components()[0]
	valFT := col.Type().Components()[1]

	// Current release restriction: string keys and string values only.
	if keyFT.Type() != stringType {
		panic(fmt.Errorf(
			"GroupIntoBatches: this release supports string keys only; got %v",
			keyFT.Type()))
	}
	if valFT.Type() != stringType {
		panic(fmt.Errorf(
			"GroupIntoBatches: this release supports string values only; got %v",
			valFT.Type()))
	}

	if !beam.NewCoder(keyFT).IsDeterministic() {
		panic(fmt.Errorf(
			"GroupIntoBatches: key coder for type %v is not deterministic.",
			keyFT.Type()))
	}

	// Byte sizer dispatch (always primitive for string).
	sizerKind := sizerNone
	if params.BatchSizeBytes > 0 {
		sizerKind = sizerPrimitive
	}

	allowedLatenessMs := int64(col.WindowingStrategy().AllowedLateness)

	fn := &groupIntoBatchesFn{
		Buffer:   state.MakeBagState[string]("batchBuffer"),
		Count:    state.MakeValueState[int64]("batchCount"),
		ByteSize: state.MakeValueState[int64]("batchBytes"),
		TimerSet: state.MakeValueState[bool]("batchTimerSet"),

		Buffering: timers.InProcessingTime("batchBuffering"),
		WindowEnd: timers.InEventTime("batchWindowEnd"),

		BatchSize:         params.BatchSize,
		BatchSizeBytes:    params.BatchSizeBytes,
		MaxBufferingMs:    params.MaxBufferingDuration.Milliseconds(),
		AllowedLatenessMs: allowedLatenessMs,
		SizerKind:         sizerKind,
	}

	return beam.ParDo(s, fn, col)
}
