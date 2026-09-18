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
	"errors"
	"strings"
	"testing"
	"time"
)

type TestOrder struct {
	ID     int64   `db:"id"`
	Region string  `db:"region"`
	Amount float64 `db:"amount"`
}

func TestBatchCompactorLastWriteWins(t *testing.T) {
	bc := NewBatchCompactor(10, 1024, 1*time.Second)

	// Add order with ID 100, amount 50.0
	bc.Add("100|", []any{int64(100)}, TestOrder{ID: 100, Region: "US", Amount: 50.0}, 64)
	// Add updated order with ID 100, amount 75.0 (should overwrite)
	bc.Add("100|", []any{int64(100)}, TestOrder{ID: 100, Region: "US", Amount: 75.0}, 64)
	// Add distinct order with ID 101
	bc.Add("101|", []any{int64(101)}, TestOrder{ID: 101, Region: "EU", Amount: 20.0}, 64)

	if bc.Len() != 2 {
		t.Fatalf("expected 2 deduplicated records, got %d", bc.Len())
	}

	records := bc.CompactAndSort()
	if len(records) != 2 {
		t.Fatalf("expected 2 compacted records, got %d", len(records))
	}

	order0 := records[0].(TestOrder)
	if order0.ID != 100 || order0.Amount != 75.0 {
		t.Errorf("expected latest amount 75.0 for ID 100, got %v", order0)
	}

	order1 := records[1].(TestOrder)
	if order1.ID != 101 || order1.Amount != 20.0 {
		t.Errorf("expected ID 101 with amount 20.0, got %v", order1)
	}
}

func TestBatchCompactorCanonicalSortingAntiDeadlock(t *testing.T) {
	bc := NewBatchCompactor(100, 1024*1024, 1*time.Second)

	// Add keys in unsorted order: 40, 10, 50, 20, 30
	ids := []int64{40, 10, 50, 20, 30}
	for _, id := range ids {
		bc.Add(string(rune(id)), []any{id}, TestOrder{ID: id, Region: "US", Amount: 10.0}, 32)
	}

	sortedRecords := bc.CompactAndSort()
	expectedOrder := []int64{10, 20, 30, 40, 50}

	for i, expectedID := range expectedOrder {
		order := sortedRecords[i].(TestOrder)
		if order.ID != expectedID {
			t.Errorf("index %d: expected ID %d, got %d", i, expectedID, order.ID)
		}
	}
}

func TestBatchCompactorCompositeKeySorting(t *testing.T) {
	bc := NewBatchCompactor(100, 1024*1024, 1*time.Second)

	// Add composite keys (ID, Region) in arbitrary order
	bc.Add("2|US", []any{int64(2), "US"}, TestOrder{ID: 2, Region: "US"}, 32)
	bc.Add("1|EU", []any{int64(1), "EU"}, TestOrder{ID: 1, Region: "EU"}, 32)
	bc.Add("1|APAC", []any{int64(1), "APAC"}, TestOrder{ID: 1, Region: "APAC"}, 32)

	sorted := bc.CompactAndSort()

	first := sorted[0].(TestOrder)
	if first.ID != 1 || first.Region != "APAC" {
		t.Errorf("expected (1, APAC), got (%d, %s)", first.ID, first.Region)
	}

	second := sorted[1].(TestOrder)
	if second.ID != 1 || second.Region != "EU" {
		t.Errorf("expected (1, EU), got (%d, %s)", second.ID, second.Region)
	}

	third := sorted[2].(TestOrder)
	if third.ID != 2 || third.Region != "US" {
		t.Errorf("expected (2, US), got (%d, %s)", third.ID, third.Region)
	}
}

func TestBatchCompactorTriTriggerThresholds(t *testing.T) {
	// 1. Element count trigger
	bcCount := NewBatchCompactor(3, 1024*1024, 1*time.Hour)
	bcCount.Add("1", []any{1}, 1, 10)
	bcCount.Add("2", []any{2}, 2, 10)
	if bcCount.ShouldFlush() {
		t.Errorf("should not flush at 2 items when max is 3")
	}
	bcCount.Add("3", []any{3}, 3, 10)
	if !bcCount.ShouldFlush() {
		t.Errorf("expected flush when reaching max count 3")
	}

	// 2. Byte volume trigger
	bcBytes := NewBatchCompactor(1000, 100, 1*time.Hour)
	bcBytes.Add("1", []any{1}, 1, 50)
	if bcBytes.ShouldFlush() {
		t.Errorf("should not flush at 50 bytes when max is 100")
	}
	bcBytes.Add("2", []any{2}, 2, 60) // total 110 bytes
	if !bcBytes.ShouldFlush() {
		t.Errorf("expected flush when exceeding max bytes 100")
	}

	// 3. Duration trigger
	bcTime := NewBatchCompactor(1000, 1024*1024, 5*time.Millisecond)
	bcTime.Add("1", []any{1}, 1, 10)
	time.Sleep(10 * time.Millisecond)
	if !bcTime.ShouldFlush() {
		t.Errorf("expected flush after flush interval exceeded")
	}
}

func TestSanitizeErrorMessage(t *testing.T) {
	err := errors.New("pq: password authentication failed for user 'beam' with password='SecretPassword123' at host 192.168.1.50 using token Bearer mockSecretToken12345")
	sanitized := SanitizeErrorMessage(err)

	if strings.Contains(sanitized, "SecretPassword123") {
		t.Errorf("sanitized message still contains plain password: %s", sanitized)
	}
	if !strings.Contains(sanitized, "password=[REDACTED]") {
		t.Errorf("expected password to be replaced with [REDACTED]: %s", sanitized)
	}
	if strings.Contains(sanitized, "192.168.1.50") {
		t.Errorf("sanitized message still contains raw IP: %s", sanitized)
	}
	if !strings.Contains(sanitized, "xxx.xxx.xxx.xxx") {
		t.Errorf("expected IP to be masked: %s", sanitized)
	}
	if strings.Contains(sanitized, "mockSecretToken12345") {
		t.Errorf("sanitized message still contains Bearer token: %s", sanitized)
	}
	if !strings.Contains(sanitized, "Bearer [REDACTED]") {
		t.Errorf("expected Bearer token to be masked: %s", sanitized)
	}
}

func TestExtractPrimaryKeys(t *testing.T) {
	order := TestOrder{ID: 42, Region: "US", Amount: 99.5}

	entityKey, sortKeys := ExtractPrimaryKeys(order, []string{"id", "region"})
	if entityKey != "42|US|" {
		t.Errorf("expected entity key '42|US|', got %q", entityKey)
	}
	if len(sortKeys) != 2 || sortKeys[0] != int64(42) || sortKeys[1] != "US" {
		t.Errorf("unexpected sort keys: %v", sortKeys)
	}
}
