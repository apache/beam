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
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
)

func init() {
	runtime.RegisterType(reflect.TypeOf((*partitionQueueTracker)(nil)))
	runtime.RegisterType(reflect.TypeOf((*PartitionQueueRestriction)(nil)).Elem())
	runtime.RegisterFunction(encodePartitionQueueRestriction)
	runtime.RegisterFunction(decodePartitionQueueRestriction)
	coder.RegisterCoder(
		reflect.TypeOf((*PartitionQueueRestriction)(nil)).Elem(),
		encodePartitionQueueRestriction,
		decodePartitionQueueRestriction,
	)
}

// PartitionWork represents a single change stream partition to be read.
// The partition's unprocessed time range is [StartTimestamp, EndTimestamp).
// After each checkpoint, StartTimestamp advances to the last claimed timestamp
// so that work resumes from where it left off.
type PartitionWork struct {
	// Token is the Spanner change stream partition token.
	// An empty token represents the root query that initialises the partition tree.
	Token          string    `json:"token"`
	StartTimestamp time.Time `json:"start"`
	// EndTimestamp is the exclusive end of the time range.
	// A zero value means this partition is unbounded (read indefinitely).
	EndTimestamp time.Time `json:"end"`
}

func (p PartitionWork) bounded() bool {
	return !p.EndTimestamp.IsZero()
}

// PartitionQueueRestriction is the SDF restriction for ReadChangeStream.
// It encodes the entire work queue of change stream partitions. The Beam
// runner serialises this restriction on every checkpoint, providing
// durable, in-memory coordination with no external state required.
//
// The first element of Pending is the partition currently being processed.
// After a checkpoint, StartTimestamp on Pending[0] reflects the last
// committed position, so the residual resumes exactly from there.
type PartitionQueueRestriction struct {
	// Pending is the ordered list of partitions to read.
	// Pending[0] is the active partition (currently being processed).
	Pending []PartitionWork `json:"pending"`
	// Bounded is true when all partitions have a finite EndTimestamp.
	// When false, IsBounded returns false and the SDF runs indefinitely.
	Bounded bool `json:"bounded"`
}

func encodePartitionQueueRestriction(r PartitionQueueRestriction) ([]byte, error) {
	return json.Marshal(r)
}

func decodePartitionQueueRestriction(b []byte) (PartitionQueueRestriction, error) {
	var r PartitionQueueRestriction
	return r, json.Unmarshal(b, &r)
}

// PartitionTimestampClaim is the position type used with partitionQueueTracker.TryClaim.
// Each call claims progress up to Timestamp within the active partition and
// optionally enqueues child partitions discovered from a PartitionStartRecord.
type PartitionTimestampClaim struct {
	// Timestamp is the latest timestamp that has been processed in the active partition.
	// The tracker advances the partition's StartTimestamp to this value so that
	// subsequent checkpoints resume from here.
	Timestamp time.Time
	// ChildPartitions are new partition tokens returned by a PartitionStartRecord.
	// They will be appended to the pending queue (the current partition's token
	// acts as their implicit parent, so no deduplication is needed in the
	// newer Spanner change stream API).
	ChildPartitions []PartitionWork
	// PartitionDone signals that the active partition has received a
	// PartitionEndRecord and should be removed from the queue.
	PartitionDone bool
}

// partitionQueueTracker is the RTracker implementation for PartitionQueueRestriction.
// It is thread-safe when wrapped in sdf.LockRTracker (required for all SDFs).
type partitionQueueTracker struct {
	rest           PartitionQueueRestriction
	stopped        bool
	err            error
	completedNanos float64 // nanoseconds of time range claimed (for GetProgress done tracking)
}

func newPartitionQueueTracker(rest PartitionQueueRestriction) *partitionQueueTracker {
	return &partitionQueueTracker{rest: rest}
}

// TryClaim advances the restriction based on the given claim.
//
//   - It advances Pending[0].StartTimestamp to claim.Timestamp so that a
//     subsequent checkpoint or split produces a residual that resumes from
//     exactly this position.
//   - If claim.ChildPartitions is non-empty, the new partitions are appended
//     to the queue (with owner-based deduplication from the caller if needed).
//   - If claim.PartitionDone is true, the active partition is removed from the queue.
//   - Returns false (stop processing) if the claim timestamp is beyond the
//     restriction's end time, or if the tracker has already stopped.
func (t *partitionQueueTracker) TryClaim(pos any) bool {
	if t.stopped {
		t.err = errors.New("TryClaim called after tracker stopped")
		return false
	}

	claim, ok := pos.(PartitionTimestampClaim)
	if !ok {
		t.stopped = true
		t.err = fmt.Errorf("TryClaim: expected PartitionTimestampClaim, got %T", pos)
		return false
	}

	if len(t.rest.Pending) == 0 {
		t.stopped = true
		return false
	}

	active := &t.rest.Pending[0]

	// Check whether the claim exceeds the partition's end time.
	if active.bounded() && !claim.Timestamp.IsZero() && claim.Timestamp.After(active.EndTimestamp) {
		t.stopped = true
		return false
	}

	// Advance the start timestamp to the claimed position.
	// This ensures that a checkpoint produces a residual that resumes from here.
	if !claim.Timestamp.IsZero() && claim.Timestamp.After(active.StartTimestamp) {
		t.completedNanos += float64(claim.Timestamp.Sub(active.StartTimestamp).Nanoseconds())
		active.StartTimestamp = claim.Timestamp
	}

	// Enqueue child partitions discovered from a PartitionStartRecord.
	// Each child partition only appears in one parent's PartitionStartRecord in the
	// newer Spanner change stream API, so no deduplication is required.
	t.rest.Pending = append(t.rest.Pending, claim.ChildPartitions...)

	if claim.PartitionDone {
		// Remove the completed partition from the front of the queue.
		t.rest.Pending = t.rest.Pending[1:]
	}

	if len(t.rest.Pending) == 0 && t.rest.Bounded {
		t.stopped = true
		return false
	}

	return true
}

// TrySplit splits the restriction into primary and residual.
//
// For fraction == 0 (self-checkpoint): the primary takes no remaining work
// (IsDone immediately returns true) and the residual continues from the
// current position (Pending[0].StartTimestamp) through the rest of the queue.
//
// For fraction > 0: the primary keeps only the active partition and the
// residual gets everything else. This aggressive split enables the runner
// to recursively distribute individual partitions across workers, achieving
// per-partition parallelism.
func (t *partitionQueueTracker) TrySplit(fraction float64) (primary, residual any, err error) {
	if t.stopped || len(t.rest.Pending) == 0 {
		return t.rest, nil, nil
	}

	if fraction == 0 {
		// Self-checkpoint: primary is done, residual takes all remaining work.
		residualRest := PartitionQueueRestriction{
			Pending: make([]PartitionWork, len(t.rest.Pending)),
			Bounded: t.rest.Bounded,
		}
		copy(residualRest.Pending, t.rest.Pending)

		// Primary becomes empty (done).
		primaryRest := PartitionQueueRestriction{
			Pending: nil,
			Bounded: t.rest.Bounded,
		}
		t.rest = primaryRest
		t.stopped = true
		return primaryRest, residualRest, nil
	}

	// Fraction > 0: split off everything except the active partition.
	// The runner can recursively split the residual further, eventually
	// producing one restriction per partition for maximum parallelism.
	if len(t.rest.Pending) <= 1 {
		return t.rest, nil, nil
	}

	residualPending := make([]PartitionWork, len(t.rest.Pending)-1)
	copy(residualPending, t.rest.Pending[1:])

	residualRest := PartitionQueueRestriction{
		Pending: residualPending,
		Bounded: t.rest.Bounded,
	}

	t.rest.Pending = t.rest.Pending[:1]
	return t.rest, residualRest, nil
}

// GetProgress returns estimated done and remaining work.
// For bounded restrictions, progress is measured in time (nanoseconds).
// For unbounded restrictions, it is measured in partition count.
func (t *partitionQueueTracker) GetProgress() (done, remaining float64) {
	if t.rest.Bounded {
		return t.timeBased()
	}
	return t.partitionBased()
}

func (t *partitionQueueTracker) timeBased() (done, remaining float64) {
	for _, p := range t.rest.Pending {
		if p.bounded() {
			remaining += float64(p.EndTimestamp.Sub(p.StartTimestamp).Nanoseconds())
		}
	}
	return t.completedNanos, remaining
}

func (t *partitionQueueTracker) partitionBased() (done, remaining float64) {
	return t.completedNanos, float64(len(t.rest.Pending))
}

// IsDone returns true when there is no pending work and the restriction is bounded.
// For unbounded restrictions it always returns false, since the SDF runs indefinitely.
func (t *partitionQueueTracker) IsDone() bool {
	if !t.rest.Bounded {
		return t.stopped && len(t.rest.Pending) == 0
	}
	return len(t.rest.Pending) == 0
}

// GetError returns any error that caused the tracker to stop.
func (t *partitionQueueTracker) GetError() error {
	return t.err
}

// GetRestriction returns the current restriction.
func (t *partitionQueueTracker) GetRestriction() any {
	return t.rest
}

// IsBounded returns whether the restriction is bounded.
func (t *partitionQueueTracker) IsBounded() bool {
	return t.rest.Bounded
}

// Ensure partitionQueueTracker implements sdf.BoundableRTracker.
var _ sdf.BoundableRTracker = (*partitionQueueTracker)(nil)
