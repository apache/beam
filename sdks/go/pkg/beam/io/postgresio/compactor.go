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
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strconv"
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
// and deterministic primary key sorting to minimize PostgreSQL SQLState 40P01 deadlocks.
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

	// Canonical sort to minimize SQLState 40P01 deadlocks
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
				sb.WriteString(formatPKPart(sortKeys[i]))
			} else {
				sortKeys[i] = nil
				sb.WriteString("nil")
			}
			sb.WriteString("|")
		}
		return sb.String(), sortKeys
	}

	if v.Kind() == reflect.Map {
		if v.IsNil() {
			return "", nil
		}
		kt := v.Type().Key()
		if kt.Kind() != reflect.String {
			return "", nil
		}
		for i, col := range pkCols {
			key := reflect.ValueOf(col)
			if key.Type() != kt {
				key = key.Convert(kt)
			}
			mapVal := v.MapIndex(key)
			if !mapVal.IsValid() {
				// reflect.Value.MapKeys returns keys in an unspecified order,
				// so a map carrying more than one spelling of the same column
				// ("ID" and "Id", say) would otherwise resolve to a different
				// entry on each call. The entity key produced here groups rows
				// for last-write-wins compaction and orders them for deadlock
				// avoidance, so an unstable choice would split one logical row
				// across bundles. Taking the lexicographically smallest match
				// makes the choice arbitrary but fixed.
				var best reflect.Value
				for _, mk := range v.MapKeys() {
					if !strings.EqualFold(mk.String(), col) {
						continue
					}
					if !best.IsValid() || mk.String() < best.String() {
						best = mk
					}
				}
				if best.IsValid() {
					mapVal = v.MapIndex(best)
				}
			}
			if mapVal.IsValid() {
				sortKeys[i] = mapVal.Interface()
				sb.WriteString(formatPKPart(sortKeys[i]))
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

// formatPKPart renders one primary key component into the canonical string
// used to group rows for compaction and to order them for deadlock avoidance.
//
// Two rows carrying the same key must always produce the same part, and two
// rows carrying different keys must never produce the same part. fmt.Sprint
// alone satisfies neither for the types PostgreSQL primary keys actually use.
//
// Width and named types are why the numeric cases dispatch on reflect.Kind
// instead of on concrete types. The same column arrives as int32 on a row
// built from a struct and int64 on a row decoded from a map, and a declared
// type such as `type OrderID int64` matches no concrete case at all. Kind sees
// through both to the underlying representation.
//
// Category prefixes are why the string "5" no longer collides with the number
// 5: a value can now only share a part with another value of its own
// category. fmt.Sprint rendered both as "5".
//
// Integral floats deliberately fall into the integer category. fmt.Sprint
// renders float64(5) as "5" already, so the collapse is not new; making it
// explicit means a numeric column decoded as float64 on one row and int64 on
// another still groups as one key, which is what PostgreSQL considers it to
// be. Non-integral floats use a shortest round-trip form so that equal values
// never differ by spelling.
//
// []byte renders as hex rather than Go's "[104 105]" slice syntax, which both
// shortens the key and keeps it free of the separator characters. time.Time is
// normalized to UTC so two identical instants recorded in different locations
// do not read as distinct keys.
func formatPKPart(v any) string {
	if v == nil {
		return "nil"
	}

	// sql.NullString, sql.NullInt64 and the driver wrappers used by pgtype all
	// carry the real key inside. Unwrap first so the value is classified on
	// what it holds rather than on the wrapper type.
	if valuer, ok := v.(driver.Valuer); ok {
		dv, err := valuer.Value()
		if err == nil {
			if dv == nil {
				return "nil"
			}
			if _, nested := dv.(driver.Valuer); !nested {
				return formatPKPart(dv)
			}
		}
	}

	switch t := v.(type) {
	case time.Time:
		return "t:" + t.UTC().Format(time.RFC3339Nano)
	case []byte:
		return "x:" + hex.EncodeToString(t)
	}

	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return "nil"
		}
		rv = rv.Elem()
	}

	switch rv.Kind() {
	case reflect.Bool:
		return "b:" + strconv.FormatBool(rv.Bool())

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "i:" + strconv.FormatInt(rv.Int(), 10)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "i:" + strconv.FormatUint(rv.Uint(), 10)

	case reflect.Float32, reflect.Float64:
		f := rv.Float()
		// Bounded well inside the int64 range: beyond it the conversion is
		// undefined, and such a value is not a plausible key anyway.
		if f == math.Trunc(f) && math.Abs(f) < 1<<62 {
			return "i:" + strconv.FormatInt(int64(f), 10)
		}
		return "f:" + strconv.FormatFloat(f, 'g', -1, 64)

	case reflect.String:
		return "s:" + escapePKPart(rv.String())
	}

	return "?:" + escapePKPart(fmt.Sprint(v))
}

// escapePKPart neutralizes the separator so that a value containing it cannot
// be read as two components.
func escapePKPart(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, `|`, `\|`)
}
