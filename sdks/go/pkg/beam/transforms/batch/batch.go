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
// GroupIntoBatches mirrors the behavior of the Java and Python
// transforms of the same name. GroupIntoBatchesWithShardedKey adds
// opaque per-element shard identifiers to the keys so the processing
// of a single hot logical key spreads across multiple workers.
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
//   - BatchSize / BatchSizeBytes are int64 (parity with proto and Java
//     long, avoiding overflow on 32-bit platforms).
//   - BatchSizeBytes is limited to primitive value types ([]byte,
//     string, numeric, bool) in this release; opaque V types panic at
//     build time if BatchSizeBytes > 0.
//   - GroupIntoBatchesWithShardedKey returns PCollection<KV<K, []V>>
//     (same shape as GroupIntoBatches), with sharding applied
//     internally. The Java/Python variants expose ShardedKey<K> to the
//     user; Go does not because the SDK's type-binding engine does not
//     accept custom generic structs as DoFn output types. The
//     cross-SDK beam:coder:sharded_key:v1 coder is nevertheless wired
//     in typex + core/graph/coder so cross-language pipelines can
//     round-trip ShardedKey values.
package batch

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/funcx"
	beamcoder "github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/timers"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/google/uuid"
)

// ShardedKey pairs a user key with an opaque shard identifier. It is
// the key type of the PCollection produced by
// GroupIntoBatchesWithShardedKey.
type ShardedKey[K any] struct {
	Key     K
	ShardID []byte
}

// RegisterShardedKeyType registers a ShardedKey[K] instantiation so
// its coder survives cross-worker serialization. Common key types
// (string, []byte, int, int64) are registered automatically at init.
// Users of other K types must call this at init time.
func RegisterShardedKeyType[K any]() {
	var zero K
	keyT := reflect.TypeOf(zero)
	skT := reflect.TypeOf(ShardedKey[K]{})

	register.DoFn3x0[K, typex.V, func(ShardedKey[K], typex.V)](&wrapShardedKeyFn[K]{})
	register.Emitter2[ShardedKey[K], typex.V]()
	beam.RegisterType(skT)

	keyEnc := beam.NewElementEncoder(keyT)
	keyDec := beam.NewElementDecoder(keyT)

	enc := func(sk ShardedKey[K]) []byte {
		var buf bytes.Buffer
		writeVarInt(&buf, int64(len(sk.ShardID)))
		buf.Write(sk.ShardID)
		if err := keyEnc.Encode(sk.Key, &buf); err != nil {
			panic(err)
		}
		return buf.Bytes()
	}
	dec := func(b []byte) ShardedKey[K] {
		r := bytes.NewReader(b)
		n := readVarInt(r)
		shardID := make([]byte, n)
		if n > 0 {
			if _, err := r.Read(shardID); err != nil {
				panic(err)
			}
		}
		k, err := keyDec.Decode(r)
		if err != nil {
			panic(err)
		}
		return ShardedKey[K]{Key: k.(K), ShardID: shardID}
	}

	// Closures inside generic functions share the same compiler
	// symbol name for every type instantiation. We wrap them with a
	// type-qualified name so the cross-worker deserializer resolves
	// the correct enc/dec for each ShardedKey[K].
	encName := fmt.Sprintf("batch.encShardedKey[%v]", keyT)
	decName := fmt.Sprintf("batch.decShardedKey[%v]", keyT)

	encFn := reflectx.MakeFuncWithName(encName, enc)
	decFn := reflectx.MakeFuncWithName(decName, dec)

	// Register in the runtime cache under the qualified name so
	// ResolveFunction finds them at deserialization time.
	runtime.RegisterFunctionWithName(encName, enc)
	runtime.RegisterFunctionWithName(decName, dec)

	encWrapped, err := funcx.New(encFn)
	if err != nil {
		panic(fmt.Sprintf("RegisterShardedKeyType: bad enc for %v: %v", skT, err))
	}
	decWrapped, err := funcx.New(decFn)
	if err != nil {
		panic(fmt.Sprintf("RegisterShardedKeyType: bad dec for %v: %v", skT, err))
	}

	beamcoder.RegisterDeterministicCoderWithFuncs(skT, encWrapped, decWrapped)
}

// Params configures GroupIntoBatches and
// GroupIntoBatchesWithShardedKey.
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

const (
	sizerNone      int32 = 0
	sizerPrimitive int32 = 1
)

// codecCache keeps a per-value-type ElementEncoder/Decoder pair.
type codecCache struct {
	once sync.Once
	enc  beam.ElementEncoder
	dec  beam.ElementDecoder
}

func (c *codecCache) init(t reflect.Type) {
	c.once.Do(func() {
		c.enc = beam.NewElementEncoder(t)
		c.dec = beam.NewElementDecoder(t)
	})
}

func (c *codecCache) encode(v any) []byte {
	var buf bytes.Buffer
	if err := c.enc.Encode(v, &buf); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (c *codecCache) decode(b []byte) any {
	v, err := c.dec.Decode(bytes.NewReader(b))
	if err != nil {
		panic(err)
	}
	return v
}

// groupIntoBatchesFn is the stateful DoFn without a processing-time
// buffering timer.
type groupIntoBatchesFn struct {
	Buffer    state.Bag[[]byte]
	Count     state.Value[int64]
	ByteSize  state.Value[int64]
	WindowEnd timers.EventTime

	ValueType beam.EncodedType

	BatchSize         int64
	BatchSizeBytes    int64
	AllowedLatenessMs int64
	SizerKind         int32

	codec codecCache
}

func (fn *groupIntoBatchesFn) ProcessElement(
	w beam.Window, sp state.Provider, tp timers.Provider,
	key typex.T, value typex.V, emit func(typex.T, []typex.V),
) {
	fn.codec.init(fn.ValueType.T)

	count, _, err := fn.Count.Read(sp)
	if err != nil {
		panic(err)
	}

	if w.MaxTimestamp() < mtime.MaxTimestamp {
		windowEnd := w.MaxTimestamp().ToTime()
		if fn.AllowedLatenessMs > 0 {
			windowEnd = windowEnd.Add(time.Duration(fn.AllowedLatenessMs) * time.Millisecond)
		}
		fn.WindowEnd.Set(tp, windowEnd, timers.WithNoOutputTimestamp())
	}

	if err := fn.Buffer.Add(sp, fn.codec.encode(value)); err != nil {
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

	if fn.BatchSize > 0 && count >= fn.BatchSize {
		fn.flush(sp, key, emit)
		return
	}
	if fn.BatchSizeBytes > 0 && newBytes >= fn.BatchSizeBytes {
		fn.flush(sp, key, emit)
		return
	}
}

func (fn *groupIntoBatchesFn) OnTimer(
	ctx context.Context, ts beam.EventTime, sp state.Provider, tp timers.Provider,
	key typex.T, timer timers.Context, emit func(typex.T, []typex.V),
) {
	if timer.Family != fn.WindowEnd.Family {
		panic(fmt.Sprintf("batch.groupIntoBatchesFn: unexpected timer family %q", timer.Family))
	}
	fn.codec.init(fn.ValueType.T)
	fn.flush(sp, key, emit)
}

func (fn *groupIntoBatchesFn) flush(
	sp state.Provider, key typex.T, emit func(typex.T, []typex.V),
) {
	buf, ok, err := fn.Buffer.Read(sp)
	if err != nil {
		panic(err)
	}
	if !ok || len(buf) == 0 {
		return
	}

	out := make([]typex.V, len(buf))
	for i, b := range buf {
		out[i] = fn.codec.decode(b)
	}
	emit(key, out)

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
}

// groupIntoBatchesBufferedFn adds a processing-time buffering timer.
type groupIntoBatchesBufferedFn struct {
	Buffer    state.Bag[[]byte]
	Count     state.Value[int64]
	ByteSize  state.Value[int64]
	TimerSet  state.Value[bool]
	Buffering timers.ProcessingTime
	WindowEnd timers.EventTime

	ValueType beam.EncodedType

	BatchSize         int64
	BatchSizeBytes    int64
	MaxBufferingMs    int64
	AllowedLatenessMs int64
	SizerKind         int32

	codec codecCache
}

func (fn *groupIntoBatchesBufferedFn) ProcessElement(
	w beam.Window, sp state.Provider, tp timers.Provider,
	key typex.T, value typex.V, emit func(typex.T, []typex.V),
) {
	fn.codec.init(fn.ValueType.T)

	count, _, err := fn.Count.Read(sp)
	if err != nil {
		panic(err)
	}

	if w.MaxTimestamp() < mtime.MaxTimestamp {
		windowEnd := w.MaxTimestamp().ToTime()
		if fn.AllowedLatenessMs > 0 {
			windowEnd = windowEnd.Add(time.Duration(fn.AllowedLatenessMs) * time.Millisecond)
		}
		fn.WindowEnd.Set(tp, windowEnd, timers.WithNoOutputTimestamp())
	}

	if err := fn.Buffer.Add(sp, fn.codec.encode(value)); err != nil {
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

	if count == 1 {
		fn.Buffering.Set(tp, time.Now().Add(time.Duration(fn.MaxBufferingMs)*time.Millisecond))
		if err := fn.TimerSet.Write(sp, true); err != nil {
			panic(err)
		}
	}

	if fn.BatchSize > 0 && count >= fn.BatchSize {
		fn.flush(sp, tp, key, emit)
		return
	}
	if fn.BatchSizeBytes > 0 && newBytes >= fn.BatchSizeBytes {
		fn.flush(sp, tp, key, emit)
		return
	}
}

func (fn *groupIntoBatchesBufferedFn) OnTimer(
	ctx context.Context, ts beam.EventTime, sp state.Provider, tp timers.Provider,
	key typex.T, timer timers.Context, emit func(typex.T, []typex.V),
) {
	fn.codec.init(fn.ValueType.T)
	switch timer.Family {
	case fn.Buffering.Family, fn.WindowEnd.Family:
		fn.flush(sp, tp, key, emit)
	default:
		panic(fmt.Sprintf(
			"batch.groupIntoBatchesBufferedFn: unexpected timer family %q", timer.Family))
	}
}

func (fn *groupIntoBatchesBufferedFn) flush(
	sp state.Provider, tp timers.Provider, key typex.T, emit func(typex.T, []typex.V),
) {
	buf, ok, err := fn.Buffer.Read(sp)
	if err != nil {
		panic(err)
	}
	if !ok || len(buf) == 0 {
		return
	}

	out := make([]typex.V, len(buf))
	for i, b := range buf {
		out[i] = fn.codec.decode(b)
	}
	emit(key, out)

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

// wrapShardedKeyFn maps KV<K, V> → KV<ShardedKey[K], V>.
type wrapShardedKeyFn[K any] struct{}

func (*wrapShardedKeyFn[K]) ProcessElement(
	key K, value typex.V, emit func(ShardedKey[K], typex.V),
) {
	emit(ShardedKey[K]{Key: key, ShardID: makeShardID()}, value)
}

var (
	workerUUIDOnce sync.Once
	workerUUIDVal  [16]byte
	shardCounter   atomic.Uint64
)

// makeShardID returns a 24-byte shard identifier: a 16-byte worker
// UUID fixed per process plus an 8-byte atomic counter, big-endian.
// The layout mirrors the Java and Python shapes exactly so the wire
// bytes of cross-language round-trips remain aligned.
func makeShardID() []byte {
	workerUUIDOnce.Do(func() {
		b, err := uuid.New().MarshalBinary()
		if err != nil {
			panic(fmt.Sprintf("batch: failed to marshal worker UUID: %v", err))
		}
		copy(workerUUIDVal[:], b)
	})
	out := make([]byte, 24)
	copy(out[:16], workerUUIDVal[:])
	counter := shardCounter.Add(1)
	binary.BigEndian.PutUint64(out[16:24], counter)
	return out
}

// writeVarInt writes a varint-encoded int64 to buf (unsigned,
// little-endian base-128).
func writeVarInt(buf *bytes.Buffer, v int64) {
	u := uint64(v)
	for u >= 0x80 {
		buf.WriteByte(byte(u) | 0x80)
		u >>= 7
	}
	buf.WriteByte(byte(u))
}

// readVarInt reads a varint-encoded int64 from r.
func readVarInt(r *bytes.Reader) int64 {
	var u uint64
	var s uint
	for {
		b, err := r.ReadByte()
		if err != nil {
			panic(err)
		}
		if b < 0x80 {
			u |= uint64(b) << s
			break
		}
		u |= uint64(b&0x7f) << s
		s += 7
	}
	return int64(u)
}

func init() {
	register.DoFn6x0[
		beam.Window, state.Provider, timers.Provider,
		typex.T, typex.V, func(typex.T, []typex.V),
	](&groupIntoBatchesFn{})
	register.DoFn6x0[
		beam.Window, state.Provider, timers.Provider,
		typex.T, typex.V, func(typex.T, []typex.V),
	](&groupIntoBatchesBufferedFn{})
	register.Emitter2[typex.T, []typex.V]()

	// Register common ShardedKey[K] types for WithShardedKey.
	RegisterShardedKeyType[string]()
	RegisterShardedKeyType[int]()
	RegisterShardedKeyType[int64]()
}

// GroupIntoBatches groups the values of the input PCollection<KV<K, V>>
// into batches of up to params.BatchSize elements (or
// params.BatchSizeBytes bytes) per key and emits them as
// PCollection<KV<K, []V>>.
//
// The input must be KV-typed. The key coder must be deterministic;
// non-deterministic key coders would corrupt state keying. Panics at
// pipeline build time on invalid params, non-KV input, zero limits, or
// a non-deterministic key coder.
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

	if !beam.NewCoder(keyFT).IsDeterministic() {
		panic(fmt.Errorf(
			"GroupIntoBatches: key coder for type %v is not deterministic. "+
				"Register a deterministic custom coder with "+
				"coder.RegisterDeterministicCoder, or use a deterministic key "+
				"type (string, []byte, bool, integer, float).", keyFT.Type()))
	}

	sizerKind := sizerNone
	if params.BatchSizeBytes > 0 {
		if !isBuiltinSizeable(valFT.Type()) {
			panic(fmt.Errorf(
				"GroupIntoBatches: BatchSizeBytes > 0 requires value type %v "+
					"to be a built-in primitive ([]byte, string, numeric, bool).",
				valFT.Type()))
		}
		sizerKind = sizerPrimitive
	}

	allowedLatenessMs := int64(col.WindowingStrategy().AllowedLateness)
	valueType := beam.EncodedType{T: valFT.Type()}

	if params.MaxBufferingDuration > 0 {
		fn := &groupIntoBatchesBufferedFn{
			Buffer:            state.MakeBagState[[]byte]("batchBuffer"),
			Count:             state.MakeValueState[int64]("batchCount"),
			ByteSize:          state.MakeValueState[int64]("batchBytes"),
			TimerSet:          state.MakeValueState[bool]("batchTimerSet"),
			Buffering:         timers.InProcessingTime("batchBuffering"),
			WindowEnd:         timers.InEventTime("batchWindowEnd"),
			ValueType:         valueType,
			BatchSize:         params.BatchSize,
			BatchSizeBytes:    params.BatchSizeBytes,
			MaxBufferingMs:    params.MaxBufferingDuration.Milliseconds(),
			AllowedLatenessMs: allowedLatenessMs,
			SizerKind:         sizerKind,
		}
		return beam.ParDo(s, fn, col)
	}

	fn := &groupIntoBatchesFn{
		Buffer:            state.MakeBagState[[]byte]("batchBuffer"),
		Count:             state.MakeValueState[int64]("batchCount"),
		ByteSize:          state.MakeValueState[int64]("batchBytes"),
		WindowEnd:         timers.InEventTime("batchWindowEnd"),
		ValueType:         valueType,
		BatchSize:         params.BatchSize,
		BatchSizeBytes:    params.BatchSizeBytes,
		AllowedLatenessMs: allowedLatenessMs,
		SizerKind:         sizerKind,
	}

	return beam.ParDo(s, fn, col)
}

// GroupIntoBatchesWithShardedKey wraps each user key with a
// ShardedKey{Key: K, ShardID: [24]byte} and then applies
// GroupIntoBatches. Output is PCollection<KV<ShardedKey[K], []V>>.
//
// The key type K must have been registered via
// RegisterShardedKeyType[K] at init time. Common types (string,
// []byte, int, int64) are registered automatically.
//
// Sharding spreads the processing of a single hot logical key across
// multiple workers: each shard is independent state, so distributed
// runners can parallelize without the user's key type changing.
func GroupIntoBatchesWithShardedKey[K any](s beam.Scope, params Params, col beam.PCollection) beam.PCollection {
	s = s.Scope("batch.GroupIntoBatchesWithShardedKey")

	if err := params.validate(); err != nil {
		panic(fmt.Errorf("GroupIntoBatchesWithShardedKey: %w", err))
	}
	if !typex.IsKV(col.Type()) {
		panic(fmt.Errorf(
			"GroupIntoBatchesWithShardedKey: input PCollection must be KV-typed; got %v",
			col.Type()))
	}
	keyFT := col.Type().Components()[0]
	var zero K
	if keyFT.Type() != reflect.TypeOf(zero) {
		panic(fmt.Errorf(
			"GroupIntoBatchesWithShardedKey: type parameter K (%v) does not match input key type (%v)",
			reflect.TypeOf(zero), keyFT.Type()))
	}

	wrapped := beam.ParDo(s, &wrapShardedKeyFn[K]{}, col)
	return GroupIntoBatches(s, params, wrapped)
}
