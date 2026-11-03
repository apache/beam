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
	"database/sql"
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
	if entityKey != "i:42|s:US|" {
		t.Errorf("expected entity key 'i:42|s:US|', got %q", entityKey)
	}
	if len(sortKeys) != 2 || sortKeys[0] != int64(42) || sortKeys[1] != "US" {
		t.Errorf("unexpected sort keys: %v", sortKeys)
	}
}

func TestExtractPrimaryKeysCollisionResistance(t *testing.T) {
	type KeyItem struct {
		PartA string `db:"part_a"`
		PartB string `db:"part_b"`
	}

	// Without escaping, ("a|b", "c") and ("a", "b|c") both yield "a|b|c|".
	// With escaping, ("a|b", "c") -> "a\|b|c|" vs ("a", "b|c") -> "a|b\|c|".
	item1 := KeyItem{PartA: "a|b", PartB: "c"}
	item2 := KeyItem{PartA: "a", PartB: "b|c"}

	key1, _ := ExtractPrimaryKeys(item1, []string{"part_a", "part_b"})
	key2, _ := ExtractPrimaryKeys(item2, []string{"part_a", "part_b"})

	if key1 == key2 {
		t.Fatalf("expected distinct keys for pipe-containing fields, but both got %q", key1)
	}

	// Test backslash escaping
	item3 := KeyItem{PartA: `a\`, PartB: "b"}
	item4 := KeyItem{PartA: "a", PartB: `\b`}
	key3, _ := ExtractPrimaryKeys(item3, []string{"part_a", "part_b"})
	key4, _ := ExtractPrimaryKeys(item4, []string{"part_a", "part_b"})

	if key3 == key4 {
		t.Fatalf("expected distinct keys for backslash-containing fields, but both got %q", key3)
	}
}

// TestFormatPKPartCollapsesEquivalentNumerics pins the property that made
// fmt.Sprint unsuitable: the same key value must produce the same part
// regardless of the Go type that happens to carry it. A column read as int32
// from a struct and int64 from a decoded map is the same row.
func TestFormatPKPartCollapsesEquivalentNumerics(t *testing.T) {
	type OrderID int64

	want := formatPKPart(int64(5))
	equivalents := []any{
		int(5), int8(5), int16(5), int32(5), int64(5),
		uint(5), uint8(5), uint16(5), uint32(5), uint64(5),
		float32(5), float64(5),
		OrderID(5),
	}

	for _, v := range equivalents {
		if got := formatPKPart(v); got != want {
			t.Errorf("formatPKPart(%T(%v)) = %q, want %q", v, v, got, want)
		}
	}
}

// TestFormatPKPartSeparatesCategories pins the other half: values that are
// genuinely different must not share a part. fmt.Sprint rendered the string
// "5", the integer 5 and the float 5.0 all as "5".
func TestFormatPKPartSeparatesCategories(t *testing.T) {
	seen := map[string]any{}
	for _, v := range []any{
		int64(5),
		"5",
		true,
		5.5,
		[]byte("5"),
		time.Unix(5, 0),
	} {
		got := formatPKPart(v)
		if prev, dup := seen[got]; dup {
			t.Errorf("formatPKPart(%T(%v)) collided with %T(%v) at %q", v, v, prev, prev, got)
		}
		seen[got] = v
	}
}

// TestFormatPKPartNormalizesTime confirms two identical instants recorded in
// different locations produce one key rather than two.
func TestFormatPKPartNormalizesTime(t *testing.T) {
	utc := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
	elsewhere := utc.In(time.FixedZone("UTC+5", 5*60*60))

	if formatPKPart(utc) != formatPKPart(elsewhere) {
		t.Errorf("the same instant produced two keys: %q vs %q",
			formatPKPart(utc), formatPKPart(elsewhere))
	}
}

// TestFormatPKPartHandlesNil confirms an absent key and an explicit nil agree,
// and that a typed nil pointer does not panic.
func TestFormatPKPartHandlesNil(t *testing.T) {
	if got := formatPKPart(nil); got != "nil" {
		t.Errorf("formatPKPart(nil) = %q, want \"nil\"", got)
	}
	if got := formatPKPart((*int64)(nil)); got != "nil" {
		t.Errorf("formatPKPart((*int64)(nil)) = %q, want \"nil\"", got)
	}
}

// TestFormatPKPartUnwrapsValuer confirms sql.NullInt64 keys on the value it
// holds rather than on the wrapper's Go rendering.
func TestFormatPKPartUnwrapsValuer(t *testing.T) {
	if got, want := formatPKPart(sql.NullInt64{Int64: 5, Valid: true}), formatPKPart(int64(5)); got != want {
		t.Errorf("wrapped key %q does not match bare key %q", got, want)
	}
	if got := formatPKPart(sql.NullInt64{Valid: false}); got != "nil" {
		t.Errorf("an invalid NullInt64 should read as NULL, got %q", got)
	}
}

// TestExtractPrimaryKeysAcceptsMapRows confirms map-shaped rows key the same
// way as their struct equivalents, which is what lets CDC rows compact.
func TestExtractPrimaryKeysAcceptsMapRows(t *testing.T) {
	structKey, _ := ExtractPrimaryKeys(TestOrder{ID: 42, Region: "US"}, []string{"id", "region"})
	mapKey, _ := ExtractPrimaryKeys(map[string]any{
		"id":     int64(42),
		"region": "US",
	}, []string{"id", "region"})

	if structKey != mapKey {
		t.Errorf("struct row keyed %q but the equivalent map row keyed %q", structKey, mapKey)
	}
}

// TestExtractPrimaryKeysMapEdgeCases confirms non-string map keys, custom string
// key types, case-insensitive keys, and nil maps do not panic.
func TestExtractPrimaryKeysMapEdgeCases(t *testing.T) {
	type CustomKey string
	customMap := map[CustomKey]any{
		"ID":     int64(10),
		"Region": "EU",
	}
	key, _ := ExtractPrimaryKeys(customMap, []string{"id", "region"})
	if key == "" {
		t.Errorf("expected valid key from custom string-typed map, got empty string")
	}

	nonStringMap := map[int]string{
		1: "one",
	}
	keyNonString, _ := ExtractPrimaryKeys(nonStringMap, []string{"id"})
	if keyNonString != "" {
		t.Errorf("expected empty key from non-string keyed map, got %q", keyNonString)
	}

	var nilMap map[string]any
	keyNil, _ := ExtractPrimaryKeys(nilMap, []string{"id"})
	if keyNil != "" {
		t.Errorf("expected empty key from nil map, got %q", keyNil)
	}
}

// TestExtractPrimaryKeysAmbiguousCaseIsStable covers the nondeterminism the
// reviewer noted in the case-insensitive fallback. reflect.Value.MapKeys
// returns keys in an unspecified order, so a map holding both "ID" and "Id"
// could resolve to a different value on each call. The entity key groups rows
// for last-write-wins compaction and orders them for deadlock avoidance, so an
// unstable choice would split one logical row across bundles and reorder locks
// between workers. The pick is arbitrary, but it has to be fixed.
func TestExtractPrimaryKeysAmbiguousCaseIsStable(t *testing.T) {
	ambiguous := map[string]any{
		"ID": int64(1),
		"Id": int64(2),
		"iD": int64(3),
	}

	first, _ := ExtractPrimaryKeys(ambiguous, []string{"id"})
	if first == "" {
		t.Fatal("expected a key from a map whose only matches differ by case")
	}
	// A single run can pass by luck; Go randomizes map iteration per range.
	for i := 0; i < 100; i++ {
		got, _ := ExtractPrimaryKeys(ambiguous, []string{"id"})
		if got != first {
			t.Fatalf("iteration %d keyed %q but the first call keyed %q; the fallback is order-dependent", i, got, first)
		}
	}

	// An exact match must still win over any case-insensitive candidate.
	withExact := map[string]any{
		"ID": int64(1),
		"id": int64(2),
	}
	exactKey, vals := ExtractPrimaryKeys(withExact, []string{"id"})
	if len(vals) != 1 || vals[0] != int64(2) {
		t.Errorf("exact key lookup returned %v (key %q), want the value under \"id\"", vals, exactKey)
	}
}
