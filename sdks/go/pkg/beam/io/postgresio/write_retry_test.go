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

package postgresio

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
)

// --- SQLSTATE extraction ---

func TestExtractSqlStateUnwraps(t *testing.T) {
	deadlock := &pq.Error{Code: pq.ErrorCode(sqlStateDeadlockDetected), Message: "deadlock detected"}

	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "nil", err: nil, want: ""},
		{name: "bare", err: deadlock, want: sqlStateDeadlockDetected},
		{
			// The staged-COPY path annotates failures, and the UNNEST
			// fallback attaches the original COPY error. A direct type
			// assertion saw only the outermost error, reported UNKNOWN, and
			// sent a retryable deadlock to the dead-letter queue.
			name: "wrapped once",
			err:  fmt.Errorf("postgresio: staged copy failed: %w", deadlock),
			want: sqlStateDeadlockDetected,
		},
		{
			name: "wrapped twice",
			err:  fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", deadlock)),
			want: sqlStateDeadlockDetected,
		},
		{
			name: "not a driver error",
			err:  errors.New("connection refused"),
			want: "UNKNOWN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractSqlState(tt.err); got != tt.want {
				t.Errorf("extractSqlState(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

// --- deadlock retry ---

func deadlockErr() error {
	return &pq.Error{Code: pq.ErrorCode(sqlStateDeadlockDetected), Message: "deadlock detected"}
}

// shortenDeadlockBackoff collapses the retry ladder so a test can walk all of
// it. At the real 50ms base the five doublings add up to roughly three
// seconds, which buys no extra coverage.
func shortenDeadlockBackoff(t *testing.T) {
	t.Helper()
	original := deadlockBaseBackoff
	deadlockBaseBackoff = time.Microsecond
	t.Cleanup(func() { deadlockBaseBackoff = original })
}

func TestRetryOnDeadlockSucceedsAfterRetry(t *testing.T) {
	attempts := 0
	retries := 0

	shortenDeadlockBackoff(t)

	err := retryOnDeadlock(context.Background(), func() { retries++ }, func(context.Context) error {
		attempts++
		if attempts < 3 {
			return deadlockErr()
		}
		return nil
	})

	if err != nil {
		t.Fatalf("retryOnDeadlock returned %v, want nil; a deadlock victim normally succeeds on replay", err)
	}
	if attempts != 3 {
		t.Errorf("op ran %d times, want 3", attempts)
	}
	if retries != 2 {
		t.Errorf("onRetry called %d times, want 2 so the metric matches the replays", retries)
	}
}

// TestRetryOnDeadlockReturnsOtherErrorsImmediately guards the cost of the
// ladder: a constraint violation will fail identically on every attempt, and
// waiting only delays the dead-letter routing that is already correct for it.
func TestRetryOnDeadlockReturnsOtherErrorsImmediately(t *testing.T) {
	attempts := 0
	want := &pq.Error{Code: "23505", Message: "duplicate key"}

	err := retryOnDeadlock(context.Background(), nil, func(context.Context) error {
		attempts++
		return want
	})

	if attempts != 1 {
		t.Errorf("op ran %d times, want 1", attempts)
	}
	if !errors.Is(err, want) {
		t.Errorf("err = %v, want the original driver error returned unchanged", err)
	}
}

func TestRetryOnDeadlockGivesUp(t *testing.T) {
	attempts := 0

	shortenDeadlockBackoff(t)

	err := retryOnDeadlock(context.Background(), nil, func(context.Context) error {
		attempts++
		return deadlockErr()
	})

	if want := deadlockMaxRetries + 1; attempts != want {
		t.Errorf("op ran %d times, want %d (the initial attempt plus %d retries)",
			attempts, want, deadlockMaxRetries)
	}
	if extractSqlState(err) != sqlStateDeadlockDetected {
		t.Errorf("err = %v, want the last deadlock error so the caller can route it", err)
	}
}

// TestRetryOnDeadlockHonoursCancellation covers the reason the sleep was
// replaced: time.Sleep ignores cancellation, so a worker draining for shutdown
// stayed alive for the remainder of the ladder and outlived its bundle.
func TestRetryOnDeadlockHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	attempts := 0
	done := make(chan error, 1)

	go func() {
		done <- retryOnDeadlock(ctx, func() { cancel() }, func(context.Context) error {
			attempts++
			return deadlockErr()
		})
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want it to wrap context.Canceled", err)
		}
		if attempts != 1 {
			t.Errorf("op ran %d times, want 1; the cancelled wait must not lead to another attempt", attempts)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retryOnDeadlock ignored cancellation and is still sleeping through the backoff ladder")
	}
}

// --- batch size estimation ---

type wideRow struct {
	ID    int64
	Tags  []string
	Blob  []byte
	Notes string
}

// TestEstimateElementSizeCountsSliceBytes covers the miscount the reviewer
// found: the previous implementation added the slice length, an element count,
// to a byte total. A []string of a hundred kilobyte values counted as 100, so
// MaxBatchBytes could be exceeded by orders of magnitude.
func TestEstimateElementSizeCountsSliceBytes(t *testing.T) {
	const (
		tagCount = 100
		tagLen   = 1024
	)

	row := wideRow{ID: 1, Tags: make([]string, tagCount)}
	for i := range row.Tags {
		row.Tags[i] = strings.Repeat("x", tagLen)
	}

	got := estimateElementSize(row)
	if min := tagCount * tagLen; got < min {
		t.Errorf("estimateElementSize = %d, want at least %d; the string payload is not being counted", got, min)
	}
}

func TestEstimateElementSizeCountsByteSlices(t *testing.T) {
	row := wideRow{ID: 1, Blob: make([]byte, 64*1024)}

	got := estimateElementSize(row)
	if got < 64*1024 {
		t.Errorf("estimateElementSize = %d, want at least %d", got, 64*1024)
	}
}

func TestEstimateElementSizeFloorsSmallRows(t *testing.T) {
	tests := []struct {
		name string
		elem any
	}{
		{name: "nil", elem: nil},
		{name: "small struct", elem: struct{ A bool }{}},
		{name: "empty string", elem: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Every row carries per-tuple overhead the Go value does not
			// show, so a batch of tiny rows must still count against
			// MaxBatchBytes at a realistic rate.
			if got := estimateElementSize(tt.elem); got != minElementSizeEstimate {
				t.Errorf("estimateElementSize = %d, want the %d floor", got, minElementSizeEstimate)
			}
		})
	}
}

// TestEstimateElementSizeHandlesNilAndPointers checks the shapes that used to
// reach the reflect calls unguarded.
func TestEstimateElementSizeHandlesNilAndPointers(t *testing.T) {
	var nilPtr *wideRow
	row := wideRow{Notes: strings.Repeat("n", 4096)}

	if got := estimateElementSize(nilPtr); got != minElementSizeEstimate {
		t.Errorf("nil pointer estimated at %d, want the %d floor", got, minElementSizeEstimate)
	}
	byValue := estimateElementSize(row)
	byPointer := estimateElementSize(&row)
	if byValue != byPointer {
		t.Errorf("a pointer estimated at %d but its pointee at %d; the two must agree or the "+
			"same data flushes at different batch sizes", byPointer, byValue)
	}
	if byValue < 4096 {
		t.Errorf("estimateElementSize = %d, want at least the 4096-byte string", byValue)
	}
}

func TestEstimateElementSizeTerminatesOnCycles(t *testing.T) {
	type node struct {
		Name string
		Next any
	}
	a := &node{Name: "a"}
	b := &node{Name: "b", Next: a}
	a.Next = b

	// Depth-bounded rather than cycle-detecting: the estimate is a budgeting
	// hint, and paying for a visited set on every element would cost more
	// than the imprecision.
	done := make(chan int, 1)
	go func() { done <- estimateElementSize(a) }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("estimateElementSize did not terminate on a cyclic value")
	}
}

// --- PgBouncer compatibility ---

// TestPgBouncerDowngradesStagedCopy covers the option that previously set a
// field nothing read. Staged COPY reuses a session-scoped staging table, which
// transaction pooling cannot keep alive between flushes.
func TestPgBouncerDowngradesStagedCopy(t *testing.T) {
	fn := &writeFn{Options: NewWriteOptions(
		WithWriteMethod(WriteMethodStagedCopy),
		WithPgBouncer(true),
	)}

	fn.applyPgBouncerCompatibility(context.Background())

	if fn.Options.WriteMethod != WriteMethodUnnest {
		t.Errorf("WriteMethod = %v, want WriteMethodUnnest; staged COPY cannot survive transaction pooling",
			fn.Options.WriteMethod)
	}
}

func TestPgBouncerLeavesOtherConfigurationsAlone(t *testing.T) {
	tests := []struct {
		name string
		opts WriteOptions
		want WriteMethod
	}{
		{
			name: "disabled",
			opts: NewWriteOptions(WithWriteMethod(WriteMethodStagedCopy), WithPgBouncer(false)),
			want: WriteMethodStagedCopy,
		},
		{
			name: "already unnest",
			opts: NewWriteOptions(WithWriteMethod(WriteMethodUnnest), WithPgBouncer(true)),
			want: WriteMethodUnnest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn := &writeFn{Options: tt.opts}
			fn.applyPgBouncerCompatibility(context.Background())
			if fn.Options.WriteMethod != tt.want {
				t.Errorf("WriteMethod = %v, want %v", fn.Options.WriteMethod, tt.want)
			}
		})
	}
}

// TestWriteRejectsMergeWithPgBouncer fails the combination at construction.
// MERGE is only expressible through staged COPY, which PgBouncer compatibility
// disables, so the pipeline would otherwise fail on the first flush of every
// worker rather than on submission.
func TestWriteRejectsMergeWithPgBouncer(t *testing.T) {
	opts := validWriteOpts()
	opts.WriteMode = WriteModeMerge
	opts.UsePgBouncer = true

	msg := writePanic(t, opts)
	if msg == "" {
		t.Fatal("Write accepted MERGE with PgBouncer compatibility, which cannot be honoured")
	}
	if !strings.Contains(msg, "PgBouncer") {
		t.Errorf("panic does not name the conflicting option: %s", msg)
	}
}

// TestWriteRejectsReplicationOriginWithPgBouncer covers the worse failure mode
// of the two: selecting an origin is session state, so under transaction
// pooling some writes would be stamped and some would not, and a bidirectional
// peer would replay back exactly the unstamped ones.
func TestWriteRejectsReplicationOriginWithPgBouncer(t *testing.T) {
	opts := validWriteOpts()
	opts.ReplicationOriginName = "beam_sink"
	opts.UsePgBouncer = true

	msg := writePanic(t, opts)
	if msg == "" {
		t.Fatal("Write accepted a replication origin with PgBouncer compatibility")
	}
	if !strings.Contains(msg, "PgBouncer") {
		t.Errorf("panic does not name the conflicting option: %s", msg)
	}
}

// TestWriteRejectsInvalidReplicationOriginName covers the path that skips the
// option setter. The cross-language SchemaTransform assigns the field from its
// configuration, so the regex in WithReplicationOriginName never ran for it.
func TestWriteRejectsInvalidReplicationOriginName(t *testing.T) {
	opts := validWriteOpts()
	opts.ReplicationOriginName = "beam sink; DROP TABLE users"

	msg := writePanic(t, opts)
	if msg == "" {
		t.Fatal("Write accepted an origin name that never passed the option validator")
	}
	if !strings.Contains(msg, "replication origin name") {
		t.Errorf("panic does not identify the bad field: %s", msg)
	}
}

func TestWriteAcceptsValidReplicationOriginName(t *testing.T) {
	opts := validWriteOpts()
	opts.ReplicationOriginName = "beam_sink_1"

	if msg := writePanic(t, opts); msg != "" {
		t.Fatalf("Write rejected a valid origin name: %s", msg)
	}
}
