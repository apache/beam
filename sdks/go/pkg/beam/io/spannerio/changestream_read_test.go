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
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---- partitionQueueTracker tests ----

func makeWork(token string, startSec, endSec int64) PartitionWork {
	var end time.Time
	if endSec > 0 {
		end = time.Unix(endSec, 0)
	}
	return PartitionWork{
		Token:          token,
		StartTimestamp: time.Unix(startSec, 0),
		EndTimestamp:   end,
	}
}

func TestTryClaim_AdvancesStartTimestamp(t *testing.T) {
	rest := PartitionQueueRestriction{
		Pending: []PartitionWork{makeWork("tok1", 100, 200)},
		Bounded: true,
	}
	tr := newPartitionQueueTracker(rest)

	claimed := tr.TryClaim(PartitionTimestampClaim{Timestamp: time.Unix(150, 0)})
	if !claimed {
		t.Fatal("expected TryClaim to return true")
	}
	if tr.rest.Pending[0].StartTimestamp != time.Unix(150, 0) {
		t.Errorf("StartTimestamp not advanced: got %v", tr.rest.Pending[0].StartTimestamp)
	}
}

func TestTryClaim_DoesNotGoBackward(t *testing.T) {
	rest := PartitionQueueRestriction{
		Pending: []PartitionWork{makeWork("tok1", 100, 200)},
		Bounded: true,
	}
	tr := newPartitionQueueTracker(rest)

	tr.TryClaim(PartitionTimestampClaim{Timestamp: time.Unix(150, 0)})
	tr.TryClaim(PartitionTimestampClaim{Timestamp: time.Unix(130, 0)}) // backward
	if tr.rest.Pending[0].StartTimestamp != time.Unix(150, 0) {
		t.Errorf("StartTimestamp went backward: got %v", tr.rest.Pending[0].StartTimestamp)
	}
}

func TestTryClaim_BeyondEndTime_ReturnsFalse(t *testing.T) {
	rest := PartitionQueueRestriction{
		Pending: []PartitionWork{makeWork("tok1", 100, 200)},
		Bounded: true,
	}
	tr := newPartitionQueueTracker(rest)

	claimed := tr.TryClaim(PartitionTimestampClaim{Timestamp: time.Unix(250, 0)})
	if claimed {
		t.Error("expected TryClaim to return false when timestamp exceeds EndTimestamp")
	}
	if !tr.stopped {
		t.Error("expected tracker to be stopped")
	}
}

func TestTryClaim_WithChildPartitions(t *testing.T) {
	rest := PartitionQueueRestriction{
		Pending: []PartitionWork{makeWork("root", 100, 0)},
		Bounded: false,
	}
	tr := newPartitionQueueTracker(rest)

	children := []PartitionWork{makeWork("child1", 150, 0), makeWork("child2", 150, 0)}
	tr.TryClaim(PartitionTimestampClaim{Timestamp: time.Unix(150, 0), ChildPartitions: children})

	if len(tr.rest.Pending) != 3 {
		t.Errorf("expected 3 pending (root + 2 children), got %d", len(tr.rest.Pending))
	}
	if tr.rest.Pending[1].Token != "child1" || tr.rest.Pending[2].Token != "child2" {
		t.Errorf("unexpected child tokens: %v", tr.rest.Pending)
	}
}

func TestTryClaim_PartitionDone_RemovesFront(t *testing.T) {
	rest := PartitionQueueRestriction{
		Pending: []PartitionWork{makeWork("tok1", 100, 200), makeWork("tok2", 100, 200)},
		Bounded: true,
	}
	tr := newPartitionQueueTracker(rest)

	claimed := tr.TryClaim(PartitionTimestampClaim{PartitionDone: true})
	if !claimed {
		t.Fatal("expected TryClaim to return true")
	}
	if len(tr.rest.Pending) != 1 {
		t.Errorf("expected 1 pending partition, got %d", len(tr.rest.Pending))
	}
	if tr.rest.Pending[0].Token != "tok2" {
		t.Errorf("expected tok2 to be next, got %q", tr.rest.Pending[0].Token)
	}
}

func TestTryClaim_AllDone_BoundedStops(t *testing.T) {
	rest := PartitionQueueRestriction{
		Pending: []PartitionWork{makeWork("tok1", 100, 200)},
		Bounded: true,
	}
	tr := newPartitionQueueTracker(rest)

	// Remove the only partition.
	claimed := tr.TryClaim(PartitionTimestampClaim{PartitionDone: true})
	if claimed {
		t.Error("expected TryClaim to return false when all bounded partitions are done")
	}
	if !tr.IsDone() {
		t.Error("expected IsDone to return true")
	}
}

func TestTryClaim_AfterStopped_ReturnsError(t *testing.T) {
	rest := PartitionQueueRestriction{
		Pending: []PartitionWork{makeWork("tok1", 100, 200)},
		Bounded: true,
	}
	tr := newPartitionQueueTracker(rest)
	tr.stopped = true

	tr.TryClaim(PartitionTimestampClaim{Timestamp: time.Unix(150, 0)})
	if tr.GetError() == nil {
		t.Error("expected error when TryClaim called after stopped")
	}
}

func TestTryClaim_WrongType_SetsError(t *testing.T) {
	rest := PartitionQueueRestriction{
		Pending: []PartitionWork{makeWork("tok1", 100, 200)},
		Bounded: true,
	}
	tr := newPartitionQueueTracker(rest)

	tr.TryClaim("not a PartitionTimestampClaim")
	if tr.GetError() == nil {
		t.Error("expected error for wrong claim type")
	}
}

// ---- TrySplit tests ----

func TestTrySplit_SelfCheckpoint_FractionZero(t *testing.T) {
	rest := PartitionQueueRestriction{
		Pending: []PartitionWork{
			makeWork("tok1", 100, 200),
			makeWork("tok2", 100, 200),
		},
		Bounded: true,
	}
	tr := newPartitionQueueTracker(rest)

	primary, residual, err := tr.TrySplit(0)
	if err != nil {
		t.Fatalf("TrySplit(0) returned error: %v", err)
	}

	prim := primary.(PartitionQueueRestriction)
	res := residual.(PartitionQueueRestriction)

	if len(prim.Pending) != 0 {
		t.Errorf("primary should be empty for self-checkpoint, got %d", len(prim.Pending))
	}
	if len(res.Pending) != 2 {
		t.Errorf("residual should have 2 partitions, got %d", len(res.Pending))
	}
	if !tr.stopped {
		t.Error("tracker should be stopped after self-checkpoint")
	}
}

func TestTrySplit_Aggressive(t *testing.T) {
	rest := PartitionQueueRestriction{
		Pending: []PartitionWork{
			makeWork("active", 100, 200),
			makeWork("tok1", 100, 200),
			makeWork("tok2", 100, 200),
			makeWork("tok3", 100, 200),
			makeWork("tok4", 100, 200),
		},
		Bounded: true,
	}
	tr := newPartitionQueueTracker(rest)

	primary, residual, err := tr.TrySplit(0.5)
	if err != nil {
		t.Fatalf("TrySplit(0.5) returned error: %v", err)
	}

	prim := primary.(PartitionQueueRestriction)
	res := residual.(PartitionQueueRestriction)

	// Aggressive split: primary keeps only the active partition.
	if len(prim.Pending) != 1 {
		t.Errorf("primary should have 1 partition (active only), got %d", len(prim.Pending))
	}
	if prim.Pending[0].Token != "active" {
		t.Errorf("active partition should remain in primary, got %q", prim.Pending[0].Token)
	}
	// Residual gets all other partitions.
	if len(res.Pending) != 4 {
		t.Errorf("residual should have 4 partitions, got %d", len(res.Pending))
	}
	if len(prim.Pending)+len(res.Pending) != 5 {
		t.Errorf("total partitions should remain 5, got primary=%d residual=%d",
			len(prim.Pending), len(res.Pending))
	}
}

func TestTrySplit_NoTail_ReturnsNilResidual(t *testing.T) {
	rest := PartitionQueueRestriction{
		Pending: []PartitionWork{makeWork("active", 100, 200)},
		Bounded: true,
	}
	tr := newPartitionQueueTracker(rest)

	primary, residual, err := tr.TrySplit(0.5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if residual != nil {
		t.Error("residual should be nil when there is only one partition")
	}
	_ = primary
}

func TestTrySplit_Stopped_ReturnsNilResidual(t *testing.T) {
	rest := PartitionQueueRestriction{
		Pending: []PartitionWork{makeWork("tok1", 100, 200)},
		Bounded: true,
	}
	tr := newPartitionQueueTracker(rest)
	tr.stopped = true

	_, residual, err := tr.TrySplit(0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if residual != nil {
		t.Error("stopped tracker should return nil residual")
	}
}

// ---- IsDone tests ----

func TestIsDone_Bounded_EmptyQueue(t *testing.T) {
	tr := newPartitionQueueTracker(PartitionQueueRestriction{Bounded: true})
	if !tr.IsDone() {
		t.Error("empty bounded queue should be done")
	}
}

func TestIsDone_Bounded_NonEmptyQueue(t *testing.T) {
	rest := PartitionQueueRestriction{
		Pending: []PartitionWork{makeWork("tok1", 100, 200)},
		Bounded: true,
	}
	tr := newPartitionQueueTracker(rest)
	if tr.IsDone() {
		t.Error("non-empty bounded queue should not be done")
	}
}

func TestIsDone_Unbounded_RequiresStopped(t *testing.T) {
	tr := newPartitionQueueTracker(PartitionQueueRestriction{Bounded: false})
	if tr.IsDone() {
		t.Error("unbounded empty queue should not be done unless stopped")
	}
	tr.stopped = true
	if !tr.IsDone() {
		t.Error("stopped unbounded empty queue should be done")
	}
}

// ---- GetProgress tests ----

func TestGetProgress_TimeBased(t *testing.T) {
	rest := PartitionQueueRestriction{
		Pending: []PartitionWork{
			makeWork("tok1", 100, 200), // 100s remaining
			makeWork("tok2", 150, 250), // 100s remaining
		},
		Bounded: true,
	}
	tr := newPartitionQueueTracker(rest)
	_, remaining := tr.GetProgress()
	wantNanos := float64(200 * time.Second.Nanoseconds())
	if remaining != wantNanos {
		t.Errorf("GetProgress remaining = %v, want %v", remaining, wantNanos)
	}
}

func TestGetProgress_PartitionBased(t *testing.T) {
	rest := PartitionQueueRestriction{
		Pending: []PartitionWork{
			makeWork("tok1", 100, 0),
			makeWork("tok2", 100, 0),
			makeWork("tok3", 100, 0),
		},
		Bounded: false,
	}
	tr := newPartitionQueueTracker(rest)
	_, remaining := tr.GetProgress()
	if remaining != 3 {
		t.Errorf("GetProgress remaining = %v, want 3", remaining)
	}
}

// ---- Coder tests ----

func TestEncodeDecodeRestriction(t *testing.T) {
	original := PartitionQueueRestriction{
		Pending: []PartitionWork{
			makeWork("tok1", 100, 200),
			makeWork("tok2", 150, 0),
		},
		Bounded: true,
	}

	encoded, err := encodePartitionQueueRestriction(original)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	decoded, err := decodePartitionQueueRestriction(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if len(decoded.Pending) != len(original.Pending) {
		t.Fatalf("decoded Pending length %d != original %d", len(decoded.Pending), len(original.Pending))
	}
	for i, p := range decoded.Pending {
		o := original.Pending[i]
		if p.Token != o.Token {
			t.Errorf("[%d] Token: got %q, want %q", i, p.Token, o.Token)
		}
		if !p.StartTimestamp.Equal(o.StartTimestamp) {
			t.Errorf("[%d] StartTimestamp: got %v, want %v", i, p.StartTimestamp, o.StartTimestamp)
		}
		if !p.EndTimestamp.Equal(o.EndTimestamp) {
			t.Errorf("[%d] EndTimestamp: got %v, want %v", i, p.EndTimestamp, o.EndTimestamp)
		}
	}
	if decoded.Bounded != original.Bounded {
		t.Errorf("Bounded: got %v, want %v", decoded.Bounded, original.Bounded)
	}
}

// ---- changeStreamWatermarkEstimator tests ----

func TestCurrentWatermark_Initial(t *testing.T) {
	we := &changeStreamWatermarkEstimator{maxObserved: math.MinInt64, minPending: math.MaxInt64}
	wm := we.CurrentWatermark()
	if !wm.IsZero() {
		t.Errorf("initial watermark should be zero time, got %v", wm)
	}
}

func TestObserveTimestamp_AdvancesWatermark(t *testing.T) {
	we := &changeStreamWatermarkEstimator{maxObserved: math.MinInt64, minPending: math.MaxInt64}
	t1 := time.Unix(1000, 0)
	we.ObserveTimestamp(t1)
	if !we.CurrentWatermark().Equal(t1) {
		t.Errorf("watermark should be %v, got %v", t1, we.CurrentWatermark())
	}
}

func TestObserveTimestamp_DoesNotGoBackward(t *testing.T) {
	we := &changeStreamWatermarkEstimator{maxObserved: math.MinInt64, minPending: math.MaxInt64}
	t1 := time.Unix(1000, 0)
	t2 := time.Unix(500, 0)
	we.ObserveTimestamp(t1)
	we.ObserveTimestamp(t2)
	if !we.CurrentWatermark().Equal(t1) {
		t.Errorf("watermark should not go backward: got %v, want %v", we.CurrentWatermark(), t1)
	}
}

func TestObserveTimestamp_AdvancesForwardOnly(t *testing.T) {
	we := &changeStreamWatermarkEstimator{maxObserved: math.MinInt64, minPending: math.MaxInt64}
	timestamps := []time.Time{
		time.Unix(100, 0),
		time.Unix(300, 0),
		time.Unix(200, 0),
		time.Unix(400, 0),
	}
	for _, ts := range timestamps {
		we.ObserveTimestamp(ts)
	}
	want := time.Unix(400, 0)
	if !we.CurrentWatermark().Equal(want) {
		t.Errorf("watermark = %v, want %v", we.CurrentWatermark(), want)
	}
}

func TestCurrentWatermark_HeldByMinPending(t *testing.T) {
	we := &changeStreamWatermarkEstimator{maxObserved: math.MinInt64, minPending: math.MaxInt64}
	// Observe a high timestamp, but set a low min-pending.
	we.ObserveTimestamp(time.Unix(1000, 0))
	we.SetMinPending(time.Unix(500, 0))
	// Watermark should be held back to min-pending.
	want := time.Unix(500, 0)
	if !we.CurrentWatermark().Equal(want) {
		t.Errorf("watermark should be held at min-pending %v, got %v", want, we.CurrentWatermark())
	}
}

func TestCurrentWatermark_MinPendingAboveObserved(t *testing.T) {
	we := &changeStreamWatermarkEstimator{maxObserved: math.MinInt64, minPending: math.MaxInt64}
	we.ObserveTimestamp(time.Unix(500, 0))
	we.SetMinPending(time.Unix(1000, 0))
	// When min-pending is above observed, watermark follows observed.
	want := time.Unix(500, 0)
	if !we.CurrentWatermark().Equal(want) {
		t.Errorf("watermark should be at observed %v, got %v", want, we.CurrentWatermark())
	}
}

func TestEncodeDecodeWatermarkState(t *testing.T) {
	maxObs := int64(1234567890)
	minPend := int64(9876543210)
	b := encodeWatermarkState(maxObs, minPend)
	gotMax, gotMin := decodeWatermarkState(b)
	if gotMax != maxObs {
		t.Errorf("maxObserved: got %d, want %d", gotMax, maxObs)
	}
	if gotMin != minPend {
		t.Errorf("minPending: got %d, want %d", gotMin, minPend)
	}
}

// ---- buildStatement tests ----

func TestBuildStatement_BoundedPartition(t *testing.T) {
	fn := &readChangeStreamFn{
		ChangeStreamName: "TestStream",
		HeartbeatMillis:  1000,
	}
	active := makeWork("mytoken", 100, 200)
	stmt := fn.buildStatement(active)

	if stmt.Params["partition_token"] != "mytoken" {
		t.Errorf("partition_token = %v, want mytoken", stmt.Params["partition_token"])
	}
	if stmt.Params["end_timestamp"] != active.EndTimestamp {
		t.Errorf("end_timestamp should be set for bounded partition")
	}
	if stmt.Params["heartbeat_milliseconds"] != int64(1000) {
		t.Errorf("heartbeat_milliseconds = %v, want 1000", stmt.Params["heartbeat_milliseconds"])
	}
}

func TestBuildStatement_RootPartition_NullToken(t *testing.T) {
	fn := &readChangeStreamFn{
		ChangeStreamName: "TestStream",
		HeartbeatMillis:  1000,
	}
	active := makeWork("", 100, 0) // empty token = root
	stmt := fn.buildStatement(active)

	if stmt.Params["partition_token"] != nil {
		t.Errorf("partition_token should be nil for root partition, got %v", stmt.Params["partition_token"])
	}
	if stmt.Params["end_timestamp"] != nil {
		t.Errorf("end_timestamp should be nil for unbounded partition, got %v", stmt.Params["end_timestamp"])
	}
}

func TestBuildStatement_ContainsStreamName(t *testing.T) {
	fn := &readChangeStreamFn{
		ChangeStreamName: "MyChangeStream",
		HeartbeatMillis:  500,
	}
	active := makeWork("tok1", 100, 200)
	stmt := fn.buildStatement(active)
	if !strings.Contains(stmt.SQL, "READ_MyChangeStream") {
		t.Errorf("SQL does not contain READ_MyChangeStream: %s", stmt.SQL)
	}
}

// ---- Stream name validation tests ----

func TestValidStreamName(t *testing.T) {
	valid := []string{"MyStream", "my_stream", "_private", "A", "Stream123"}
	for _, name := range valid {
		if !validStreamName.MatchString(name) {
			t.Errorf("expected %q to be valid", name)
		}
	}
	invalid := []string{"", "123abc", "my-stream", "my stream", "a.b", "SELECT 1; --"}
	for _, name := range invalid {
		if validStreamName.MatchString(name) {
			t.Errorf("expected %q to be invalid", name)
		}
	}
}

// ---- GetProgress done tracking tests ----

func TestGetProgress_TracksDone(t *testing.T) {
	rest := PartitionQueueRestriction{
		Pending: []PartitionWork{makeWork("tok1", 100, 200)},
		Bounded: true,
	}
	tr := newPartitionQueueTracker(rest)

	// Initially no done work.
	done, _ := tr.GetProgress()
	if done != 0 {
		t.Errorf("initial done should be 0, got %v", done)
	}

	// Claim a timestamp 50s ahead of start (100 → 150).
	tr.TryClaim(PartitionTimestampClaim{Timestamp: time.Unix(150, 0)})
	done, _ = tr.GetProgress()
	wantNanos := float64(50 * time.Second.Nanoseconds())
	if done != wantNanos {
		t.Errorf("done after claim = %v, want %v", done, wantNanos)
	}
}

// ---- isTransientError tests ----

func TestIsTransientError(t *testing.T) {
	if !isTransientError(status.Error(codes.Unavailable, "service unavailable")) {
		t.Error("UNAVAILABLE should be transient")
	}
	if !isTransientError(status.Error(codes.Aborted, "transaction aborted")) {
		t.Error("ABORTED should be transient")
	}
	if isTransientError(status.Error(codes.NotFound, "not found")) {
		t.Error("NOT_FOUND should not be transient")
	}
	if isTransientError(status.Error(codes.PermissionDenied, "denied")) {
		t.Error("PERMISSION_DENIED should not be transient")
	}
	if isTransientError(fmt.Errorf("plain error")) {
		t.Error("non-gRPC error should not be transient")
	}
}
