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
	"bytes"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"
)

var (
	passwordRegex = regexp.MustCompile(`(?i)(password|token|secret|key|access_token|client_secret)\s*[:=]\s*['"]?[^\s,'"]+['"]?`)
	urlCredRegex  = regexp.MustCompile(`(?i)([a-zA-Z][a-zA-Z0-9+.-]*)://([^:]+):([^@]+)@`)
	bearerRegex   = regexp.MustCompile(`(?i)Bearer\s+[a-zA-Z0-9_\-\.~+/]+=*`)
	ipRegex       = regexp.MustCompile(`\b(?:\d{1,3}\.){3}\d{1,3}\b`)
)

// RecordEntry represents an in-flight element buffered within a micro-batch.
type RecordEntry struct {
	EntityKey  string
	SortKey    []any
	Data       any
	ByteLength int
}

// BatchCompactor manages micro-batch buffering, Last-Write-Wins (LWW) deduplication,
// and deterministic primary key sorting to prevent PostgreSQL SQLState 40P01 deadlocks.
type BatchCompactor struct {
	maxBatchSize  int
	maxBatchBytes int
	flushInterval time.Duration
	lastFlushTime time.Time

	buffer       []RecordEntry
	seenKeys     map[string]int // maps entity key to slice index for LWW deduplication
	currentBytes int
}

// NewBatchCompactor initializes a BatchCompactor with configured threshold limits.
func NewBatchCompactor(maxBatchSize, maxBatchBytes int, flushInterval time.Duration) *BatchCompactor {
	if maxBatchSize <= 0 {
		maxBatchSize = 5000
	}
	if maxBatchBytes <= 0 {
		maxBatchBytes = 8 * 1024 * 1024
	}
	if flushInterval <= 0 {
		flushInterval = 1 * time.Second
	}
	return &BatchCompactor{
		maxBatchSize:  maxBatchSize,
		maxBatchBytes: maxBatchBytes,
		flushInterval: flushInterval,
		lastFlushTime: time.Now(),
		buffer:        make([]RecordEntry, 0, maxBatchSize),
		seenKeys:      make(map[string]int),
	}
}

// Add buffers an entry with Last-Write-Wins deduplication if entityKey is non-empty.
func (bc *BatchCompactor) Add(entityKey string, sortKey []any, data any, byteLength int) {
	if byteLength <= 0 {
		byteLength = 64
	}

	// Sort-key-aware last-write-wins deduplication.
	//
	// Beam provides no ordering guarantee between bundles, and an upstream
	// shuffle or a replay can deliver an older mutation after a newer one.
	// Overwriting unconditionally would discard the newer row and leave the
	// sink holding a stale value that no later event corrects.
	//
	// A strictly older incoming record is therefore dropped. Equal sort keys
	// still overwrite: the sink populates SortKey with the primary key for
	// canonical flush ordering, so two updates to the same row carry the same
	// key and carry no recency information, and plain arrival order remains
	// the best available answer. When the CDC path populates SortKey with
	// (LSN, intra-transaction sequence) the comparison becomes a true recency
	// check.
	if entityKey != "" {
		if existingIdx, exists := bc.seenKeys[entityKey]; exists {
			existing := bc.buffer[existingIdx]
			if compareSortKeys(sortKey, existing.SortKey) < 0 {
				// The buffered record is strictly newer: keep it.
				return
			}
			bc.currentBytes -= existing.ByteLength
			bc.buffer[existingIdx] = RecordEntry{
				EntityKey:  entityKey,
				SortKey:    sortKey,
				Data:       data,
				ByteLength: byteLength,
			}
			bc.currentBytes += byteLength
			return
		}
		bc.seenKeys[entityKey] = len(bc.buffer)
	}

	bc.buffer = append(bc.buffer, RecordEntry{
		EntityKey:  entityKey,
		SortKey:    sortKey,
		Data:       data,
		ByteLength: byteLength,
	})
	bc.currentBytes += byteLength
}

// ShouldFlush evaluates the tri-trigger flush conditions: element count, byte volume, or duration.
func (bc *BatchCompactor) ShouldFlush() bool {
	if len(bc.buffer) == 0 {
		return false
	}
	if len(bc.buffer) >= bc.maxBatchSize {
		return true
	}
	if bc.currentBytes >= bc.maxBatchBytes {
		return true
	}
	if time.Since(bc.lastFlushTime) >= bc.flushInterval {
		return true
	}
	return false
}

// CompactAndSort returns the deduplicated records sorted canonically by composite primary key,
// and resets the internal buffer for the next micro-batch.
func (bc *BatchCompactor) CompactAndSort() []any {
	if len(bc.buffer) == 0 {
		return nil
	}

	// Canonical sort to eliminate SQLState 40P01 deadlocks
	sort.SliceStable(bc.buffer, func(i, j int) bool {
		return compareSortKeys(bc.buffer[i].SortKey, bc.buffer[j].SortKey) < 0
	})

	result := make([]any, len(bc.buffer))
	for i, entry := range bc.buffer {
		result[i] = entry.Data
	}

	// Reset buffer
	bc.buffer = bc.buffer[:0]
	bc.seenKeys = make(map[string]int)
	bc.currentBytes = 0
	bc.lastFlushTime = time.Now()

	return result
}

// Len returns the current number of buffered elements.
func (bc *BatchCompactor) Len() int {
	return len(bc.buffer)
}

// compareSortKeys performs deterministic lexicographical comparison across composite key elements.
func compareSortKeys(a, b []any) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		cmp := compareSingleKey(a[i], b[i])
		if cmp != 0 {
			return cmp
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

func compareSingleKey(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	switch va := a.(type) {
	case int:
		if vb, ok := b.(int); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case int64:
		if vb, ok := b.(int64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case string:
		if vb, ok := b.(string); ok {
			return strings.Compare(va, vb)
		}
	case float64:
		if vb, ok := b.(float64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case []byte:
		if vb, ok := b.([]byte); ok {
			return bytes.Compare(va, vb)
		}
	}

	// Fallback to string representation for other types
	return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
}

// SanitizeErrorMessage scrubs passwords, inline JDBC URL credentials, Bearer tokens,
// and IP addresses from error strings prior to emission to metrics or Dead-Letter Queues.
func SanitizeErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	msg = passwordRegex.ReplaceAllString(msg, "$1=[REDACTED]")
	msg = urlCredRegex.ReplaceAllString(msg, "$1://[REDACTED]:[REDACTED]@")
	msg = bearerRegex.ReplaceAllString(msg, "Bearer [REDACTED]")
	msg = ipRegex.ReplaceAllString(msg, "xxx.xxx.xxx.xxx")
	return msg
}

// ExtractPrimaryKeys extracts primary key values from a struct or map based on column names.
func ExtractPrimaryKeys(val any, pkCols []string) (string, []any) {
	if len(pkCols) == 0 {
		return "", nil
	}

	sortKeys := make([]any, len(pkCols))
	var sb strings.Builder

	v := reflect.ValueOf(val)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() == reflect.Struct {
		for i, col := range pkCols {
			f := v.FieldByName(col)
			if !f.IsValid() {
				// Case-insensitive match or db tag match
				t := v.Type()
				for j := 0; j < t.NumField(); j++ {
					fld := t.Field(j)
					if strings.EqualFold(fld.Name, col) || fld.Tag.Get("db") == col {
						f = v.Field(j)
						break
					}
				}
			}
			if f.IsValid() {
				sortKeys[i] = f.Interface()
				sb.WriteString(fmt.Sprint(sortKeys[i]))
			} else {
				sortKeys[i] = nil
				sb.WriteString("nil")
			}
			sb.WriteString("|")
		}
		return sb.String(), sortKeys
	}

	if v.Kind() == reflect.Map {
		for i, col := range pkCols {
			mapVal := v.MapIndex(reflect.ValueOf(col))
			if mapVal.IsValid() {
				sortKeys[i] = mapVal.Interface()
				sb.WriteString(fmt.Sprint(sortKeys[i]))
			} else {
				sortKeys[i] = nil
				sb.WriteString("nil")
			}
			sb.WriteString("|")
		}
		return sb.String(), sortKeys
	}

	return "", nil
}
