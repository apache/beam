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

package spannerio

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
)

// changeStreamWatermarkEstimator tracks the watermark for the ReadChangeStream SDF.
// It advances as DataChangeRecords and HeartbeatRecords are observed, but is held
// back to the minimum start timestamp of all pending (unprocessed) partitions in
// the restriction. This prevents the watermark from advancing past partitions that
// have not yet been read, which would cause their records to arrive as late data.
//
// With aggressive TrySplit, most restrictions contain a single partition, so
// minPending == the partition's current start timestamp in practice.
type changeStreamWatermarkEstimator struct {
	// maxObserved holds the highest observed timestamp as Unix nanoseconds.
	// math.MinInt64 means "not yet advanced".
	maxObserved int64
	// minPending holds the minimum start timestamp of all pending partitions
	// as Unix nanoseconds. math.MaxInt64 means "no pending partitions".
	minPending int64
}

// watermarkStateSize is the byte size of the serialised watermark estimator state
// (two int64 values encoded as little-endian).
const watermarkStateSize = 16

// CurrentWatermark returns the current watermark time. It is the minimum of the
// highest observed commit/heartbeat timestamp and the earliest pending partition
// start timestamp. This ensures the watermark never advances past data that has
// not yet been emitted.
func (e *changeStreamWatermarkEstimator) CurrentWatermark() time.Time {
	wm := e.maxObserved
	if e.minPending < wm {
		wm = e.minPending
	}
	if wm == math.MinInt64 || wm == math.MaxInt64 {
		return time.Time{} // zero time = beginning of time
	}
	return time.Unix(0, wm)
}

// ObserveTimestamp advances the max-observed watermark to t if t is later.
func (e *changeStreamWatermarkEstimator) ObserveTimestamp(t time.Time) {
	ns := t.UnixNano()
	if ns > e.maxObserved {
		e.maxObserved = ns
	}
}

// SetMinPending updates the minimum pending partition start timestamp. This is
// called at the start of each ProcessElement invocation with the minimum start
// timestamp from the restriction's pending partition queue.
func (e *changeStreamWatermarkEstimator) SetMinPending(t time.Time) {
	e.minPending = t.UnixNano()
}

// encodeWatermarkState serialises the watermark estimator state to bytes.
func encodeWatermarkState(maxObserved, minPending int64) []byte {
	b := make([]byte, watermarkStateSize)
	binary.LittleEndian.PutUint64(b[0:8], uint64(maxObserved))
	binary.LittleEndian.PutUint64(b[8:16], uint64(minPending))
	return b
}

// decodeWatermarkState deserialises the watermark estimator state from bytes.
// Returns (maxObserved, minPending).
func decodeWatermarkState(b []byte) (int64, int64) {
	if len(b) < watermarkStateSize {
		return math.MinInt64, math.MaxInt64
	}
	maxObserved := int64(binary.LittleEndian.Uint64(b[0:8]))
	minPending := int64(binary.LittleEndian.Uint64(b[8:16]))
	return maxObserved, minPending
}

// Compile-time check: changeStreamWatermarkEstimator implements sdf.TimestampObservingEstimator.
var _ sdf.TimestampObservingEstimator = (*changeStreamWatermarkEstimator)(nil)
