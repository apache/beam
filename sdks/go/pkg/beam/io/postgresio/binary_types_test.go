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
	"encoding/binary"
	"strings"
	"testing"
	"time"
)

// encodeNumeric builds a PostgreSQL binary NUMERIC payload.
//
// Used to construct wire inputs for the decoder tests. The layout mirrors
// PostgreSQL's own: int16 ndigits, int16 weight, uint16 sign, int16 dscale,
// then base-10000 digit words.
func encodeNumeric(weight int16, sign uint16, dscale int16, digits []uint16) []byte {
	b := make([]byte, 0, 8+len(digits)*2)
	b = binary.BigEndian.AppendUint16(b, uint16(len(digits)))
	b = binary.BigEndian.AppendUint16(b, uint16(weight))
	b = binary.BigEndian.AppendUint16(b, sign)
	b = binary.BigEndian.AppendUint16(b, uint16(dscale))
	for _, d := range digits {
		b = binary.BigEndian.AppendUint16(b, d)
	}
	return b
}

// TestDecodeBinaryNumeric covers the representative shapes PostgreSQL emits.
//
// FINDING P1-3. Before this decoder existed, every one of these inputs was
// returned to the pipeline as an opaque []byte.
func TestDecodeBinaryNumeric(t *testing.T) {
	tests := []struct {
		name   string
		weight int16
		sign   uint16
		dscale int16
		digits []uint16
		want   string
	}{
		// 123.45 -> digit words [123][4500], weight 0, dscale 2
		{"typical decimal", 0, numericPositive, 2, []uint16{123, 4500}, "123.45"},
		// -123.45
		{"negative", 0, numericNegative, 2, []uint16{123, 4500}, "-123.45"},
		// 0 is encoded with no digit words
		{"zero", 0, numericPositive, 0, nil, "0"},
		{"zero with scale", 0, numericPositive, 2, nil, "0.00"},
		// Integer with no fractional part: 1234 -> weight 0, [1234]
		{"small integer", 0, numericPositive, 0, []uint16{1234}, "1234"},
		// 100000000 -> weight 2, words [1][0000][0000]
		{"large integer", 2, numericPositive, 0, []uint16{1, 0, 0}, "100000000"},
		// Monetary value with two decimals: 19.99
		{"money", 0, numericPositive, 2, []uint16{19, 9900}, "19.99"},
		// Trailing zeros must be preserved to dscale: 5.10
		{"preserves trailing zero", 0, numericPositive, 2, []uint16{5, 1000}, "5.10"},
		// Special values
		{"NaN", 0, numericNaN, 0, nil, "NaN"},
		{"positive infinity", 0, numericPosInf, 0, nil, "Infinity"},
		{"negative infinity", 0, numericNegInf, 0, nil, "-Infinity"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeBinaryNumeric(encodeNumeric(tc.weight, tc.sign, tc.dscale, tc.digits))
			if err != nil {
				t.Fatalf("decodeBinaryNumeric() error: %v", err)
			}
			if got != tc.want {
				t.Errorf("decodeBinaryNumeric() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestDecodeBinaryNumericPreservesPrecisionBeyondFloat64 is the core reason
// NUMERIC is returned as a string.
//
// A float64 carries about 15-17 significant decimal digits. A NUMERIC with
// more than that would be silently rounded, which for a financial workload is
// data corruption that no downstream check would notice.
func TestDecodeBinaryNumericPreservesPrecisionBeyondFloat64(t *testing.T) {
	// 1234567890123456789012.5 -- 23 significant digits.
	//
	// Base-10000 words align from the right, so the leading word carries only
	// the remaining 2 digits: 12|3456|7890|1234|5678|9012 then .5000
	// weight 5 means the first six words form the integer part.
	digits := []uint16{12, 3456, 7890, 1234, 5678, 9012, 5000}

	got, err := decodeBinaryNumeric(encodeNumeric(5, numericPositive, 1, digits))
	if err != nil {
		t.Fatalf("decodeBinaryNumeric() error: %v", err)
	}

	const want = "1234567890123456789012.5"
	if got != want {
		t.Errorf("decodeBinaryNumeric() = %q, want %q", got, want)
	}
}

// TestDecodeBinaryNumericRejectsMalformedInput ensures a truncated or corrupt
// payload produces an error rather than a wrong number.
func TestDecodeBinaryNumericRejectsMalformedInput(t *testing.T) {
	cases := map[string][]byte{
		"empty":            {},
		"truncated header": {0, 1, 0, 0},
		"truncated digits": {0, 2, 0, 0, 0, 0, 0, 0, 0, 1}, // declares 2 words, supplies 1
	}

	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeBinaryNumeric(payload); err == nil {
				t.Error("decodeBinaryNumeric() accepted malformed input")
			}
		})
	}
}

// --- UUID ---

func TestDecodeBinaryUUID(t *testing.T) {
	raw := []byte{
		0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8,
		0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38, 0x0a, 0x11,
	}

	got, err := decodeBinaryUUID(raw)
	if err != nil {
		t.Fatalf("decodeBinaryUUID() error: %v", err)
	}

	const want = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	if got != want {
		t.Errorf("decodeBinaryUUID() = %q, want %q", got, want)
	}
}

func TestDecodeBinaryUUIDRejectsWrongLength(t *testing.T) {
	for _, n := range []int{0, 15, 17, 32} {
		if _, err := decodeBinaryUUID(make([]byte, n)); err == nil {
			t.Errorf("decodeBinaryUUID() accepted a %d-byte payload", n)
		}
	}
}

// --- INTERVAL ---

func encodeInterval(micros int64, days, months int32) []byte {
	b := make([]byte, 0, 16)
	b = binary.BigEndian.AppendUint64(b, uint64(micros))
	b = binary.BigEndian.AppendUint32(b, uint32(days))
	b = binary.BigEndian.AppendUint32(b, uint32(months))
	return b
}

func TestDecodeBinaryInterval(t *testing.T) {
	tests := []struct {
		name    string
		micros  int64
		days    int32
		months  int32
		wantStr string
	}{
		{"one hour", 3_600_000_000, 0, 0, "01:00:00"},
		{"one day", 0, 1, 0, "1 day"},
		{"one month", 0, 0, 1, "1 mon"},
		{"one year", 0, 0, 12, "1 year"},
		{"composite", 3_600_000_000, 2, 14, "1 year 2 mons 2 days 01:00:00"},
		{"fractional seconds", 1_500_000, 0, 0, "00:00:01.500000"},
		{"zero", 0, 0, 0, "00:00:00"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			iv, err := decodeBinaryInterval(encodeInterval(tc.micros, tc.days, tc.months))
			if err != nil {
				t.Fatalf("decodeBinaryInterval() error: %v", err)
			}

			if iv.Months != tc.months || iv.Days != tc.days || iv.Microthings != tc.micros {
				t.Errorf("decodeBinaryInterval() = %+v, want months=%d days=%d micros=%d",
					iv, tc.months, tc.days, tc.micros)
			}
			if got := iv.String(); got != tc.wantStr {
				t.Errorf("PgInterval.String() = %q, want %q", got, tc.wantStr)
			}
		})
	}
}

// TestIntervalKeepsMonthsAndDaysSeparate documents why the interval is not
// collapsed into a time.Duration: a month is not a fixed number of days and a
// day is not always 24 hours.
func TestIntervalKeepsMonthsAndDaysSeparate(t *testing.T) {
	oneMonth, err := decodeBinaryInterval(encodeInterval(0, 0, 1))
	if err != nil {
		t.Fatalf("decodeBinaryInterval() error: %v", err)
	}
	thirtyDays, err := decodeBinaryInterval(encodeInterval(0, 30, 0))
	if err != nil {
		t.Fatalf("decodeBinaryInterval() error: %v", err)
	}

	if oneMonth.Months == thirtyDays.Months && oneMonth.Days == thirtyDays.Days {
		t.Error("'1 month' and '30 days' decoded to the same components; PostgreSQL treats them as distinct")
	}
}

func TestDecodeBinaryIntervalRejectsWrongLength(t *testing.T) {
	for _, n := range []int{0, 8, 12, 20} {
		if _, err := decodeBinaryInterval(make([]byte, n)); err == nil {
			t.Errorf("decodeBinaryInterval() accepted a %d-byte payload", n)
		}
	}
}

func TestPgVectorBinaryEncodeDecode(t *testing.T) {
	orig := Vector{0.125, -1.5, 42.0, 0.0, 3.1415927}
	encoded := EncodeBinaryVector(orig)

	decoded, err := DecodeBinaryVector(encoded)
	if err != nil {
		t.Fatalf("unexpected error decoding vector: %v", err)
	}
	if len(decoded) != len(orig) {
		t.Fatalf("dimension mismatch: got %d, want %d", len(decoded), len(orig))
	}
	for i := range orig {
		if decoded[i] != orig[i] {
			t.Errorf("element %d mismatch: got %f, want %f", i, decoded[i], orig[i])
		}
	}
}

func TestPgVectorDecodeRejectsInvalidHeader(t *testing.T) {
	t.Run("short payload", func(t *testing.T) {
		_, err := DecodeBinaryVector([]byte{0x00, 0x02})
		if err == nil {
			t.Errorf("expected error on short payload")
		}
	})

	t.Run("non-zero unused word", func(t *testing.T) {
		// dim = 1, unused = 1
		payload := []byte{0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}
		_, err := DecodeBinaryVector(payload)
		if err == nil {
			t.Errorf("expected error for non-zero unused header word")
		}
	})

	t.Run("truncated data", func(t *testing.T) {
		// dim = 3, expects 4 + 12 = 16 bytes, but only 8 provided
		payload := []byte{0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
		_, err := DecodeBinaryVector(payload)
		if err == nil {
			t.Errorf("expected error for truncated vector data")
		}
	})
}

func TestPgVectorFormatting(t *testing.T) {
	vec := Vector{1.5, -2.0, 3.25}
	formatted := FormatVectorLiteral(vec)
	expected := "[1.5,-2,3.25]"
	if formatted != expected {
		t.Errorf("expected %q, got %q", expected, formatted)
	}
	if vec.String() != expected {
		t.Errorf("expected String() %q, got %q", expected, vec.String())
	}

	emptyVec := Vector{}
	if FormatVectorLiteral(emptyVec) != "[]" {
		t.Errorf("expected [], got %q", FormatVectorLiteral(emptyVec))
	}
}

func TestPgIntervalDurationAndString(t *testing.T) {
	iv := PgInterval{
		Months:      2,
		Days:        5,
		Microthings: int64(3 * time.Hour / time.Microsecond),
	}

	dur := iv.Duration()
	expectedDays := int64(2*30 + 5)
	expectedDur := time.Duration(expectedDays)*24*time.Hour + 3*time.Hour
	if dur != expectedDur {
		t.Errorf("expected duration %v, got %v", expectedDur, dur)
	}

	s := iv.String()
	if !strings.Contains(s, "2 mons") || !strings.Contains(s, "5 days") {
		t.Errorf("unexpected interval string: %s", s)
	}
}
