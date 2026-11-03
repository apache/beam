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
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// PostgreSQL binary NUMERIC sign words.
const (
	numericPositive = 0x0000
	numericNegative = 0x4000
	numericNaN      = 0xC000
	numericPosInf   = 0xD000
	numericNegInf   = 0xF000
)

// PgInterval represents a PostgreSQL INTERVAL.
//
// PostgreSQL stores an interval as three independent components rather than a
// single duration, because months and days are not fixed-length: a month may
// be 28-31 days and a day may be 23-25 hours across a DST boundary. Collapsing
// them into a time.Duration would silently lose that distinction, so the
// components are preserved.
type PgInterval struct {
	Months       int32
	Days         int32
	Microseconds int64 // microseconds within the day
}

// String renders the interval in PostgreSQL's standard output format.
func (iv PgInterval) String() string {
	var parts []string
	if iv.Months != 0 {
		years := iv.Months / 12
		months := iv.Months % 12
		if years != 0 {
			parts = append(parts, fmt.Sprintf("%d year%s", years, plural(int64(years))))
		}
		if months != 0 {
			parts = append(parts, fmt.Sprintf("%d mon%s", months, plural(int64(months))))
		}
	}
	if iv.Days != 0 {
		parts = append(parts, fmt.Sprintf("%d day%s", iv.Days, plural(int64(iv.Days))))
	}

	if iv.Microseconds != 0 || len(parts) == 0 {
		micros := iv.Microseconds
		neg := micros < 0
		if neg {
			micros = -micros
		}
		hours := micros / 3_600_000_000
		micros -= hours * 3_600_000_000
		minutes := micros / 60_000_000
		micros -= minutes * 60_000_000
		seconds := micros / 1_000_000
		frac := micros - seconds*1_000_000

		sign := ""
		if neg {
			sign = "-"
		}
		if frac != 0 {
			parts = append(parts, fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hours, minutes, seconds, frac))
		} else {
			parts = append(parts, fmt.Sprintf("%s%02d:%02d:%02d", sign, hours, minutes, seconds))
		}
	}

	return strings.Join(parts, " ")
}

// Duration converts the interval to a time.Duration using the nominal
// conversions of 30 days per month and 24 hours per day.
//
// This is lossy by definition and is provided only for callers that need an
// approximate magnitude. Prefer the individual components for correctness.
func (iv PgInterval) Duration() time.Duration {
	days := int64(iv.Months)*30 + int64(iv.Days)
	return time.Duration(days)*24*time.Hour + time.Duration(iv.Microseconds)*time.Microsecond
}

func plural(n int64) string {
	if n == 1 || n == -1 {
		return ""
	}
	return "s"
}

// decodeBinaryNumeric decodes the PostgreSQL binary NUMERIC wire format.
//
// Layout: int16 ndigits, int16 weight, uint16 sign, int16 dscale, followed by
// ndigits base-10000 words. weight is the base-10000 exponent of the first
// word, and dscale is the number of decimal digits to display after the point.
//
// The value is returned as a string so that arbitrary precision survives. A
// float64 cannot represent a NUMERIC exactly, and silently rounding monetary
// values is the failure this decoder exists to prevent.
func decodeBinaryNumeric(b []byte) (string, error) {
	if len(b) < 8 {
		return "", fmt.Errorf("numeric: header requires 8 bytes, got %d", len(b))
	}

	ndigits := int(int16(binary.BigEndian.Uint16(b[0:2])))
	weight := int(int16(binary.BigEndian.Uint16(b[2:4])))
	sign := binary.BigEndian.Uint16(b[4:6])
	dscale := int(int16(binary.BigEndian.Uint16(b[6:8])))

	switch sign {
	case numericNaN:
		return "NaN", nil
	case numericPosInf:
		return "Infinity", nil
	case numericNegInf:
		return "-Infinity", nil
	case numericPositive, numericNegative:
		// Normal value.
	default:
		return "", fmt.Errorf("numeric: unknown sign word 0x%04x", sign)
	}

	if ndigits < 0 {
		return "", fmt.Errorf("numeric: negative digit count %d", ndigits)
	}
	if len(b) < 8+ndigits*2 {
		return "", fmt.Errorf("numeric: declared %d digit words but only %d bytes remain", ndigits, len(b)-8)
	}
	if dscale < 0 {
		return "", fmt.Errorf("numeric: negative display scale %d", dscale)
	}

	digits := make([]uint16, ndigits)
	for i := 0; i < ndigits; i++ {
		digits[i] = binary.BigEndian.Uint16(b[8+i*2 : 10+i*2])
		if digits[i] > 9999 {
			return "", fmt.Errorf("numeric: digit word %d out of base-10000 range: %d", i, digits[i])
		}
	}

	// Zero is encoded with no digit words.
	if ndigits == 0 {
		if dscale > 0 {
			return "0." + strings.Repeat("0", dscale), nil
		}
		return "0", nil
	}

	var intPart strings.Builder
	// Digit words with index <= weight contribute to the integer part.
	for i := 0; i <= weight; i++ {
		var word uint16
		if i < ndigits {
			word = digits[i]
		}
		if i == 0 {
			intPart.WriteString(strconv.Itoa(int(word)))
		} else {
			intPart.WriteString(fmt.Sprintf("%04d", word))
		}
	}
	if intPart.Len() == 0 {
		intPart.WriteString("0")
	}

	var fracPart strings.Builder
	for i := weight + 1; i < ndigits; i++ {
		if i < 0 {
			continue
		}
		fracPart.WriteString(fmt.Sprintf("%04d", digits[i]))
	}
	// A negative weight means leading zero groups before the first digit word.
	if weight < -1 {
		fracPart.Reset()
		for i := 0; i < -weight-1; i++ {
			fracPart.WriteString("0000")
		}
		for i := 0; i < ndigits; i++ {
			fracPart.WriteString(fmt.Sprintf("%04d", digits[i]))
		}
	}

	frac := fracPart.String()
	// dscale is authoritative for the displayed precision.
	if len(frac) < dscale {
		frac += strings.Repeat("0", dscale-len(frac))
	}
	frac = frac[:dscale]

	out := intPart.String()
	if dscale > 0 {
		out += "." + frac
	}
	if sign == numericNegative && strings.Trim(out, "0.") != "" {
		out = "-" + out
	}

	return out, nil
}

// decodeBinaryUUID renders the 16-byte binary UUID in canonical 8-4-4-4-12 form.
func decodeBinaryUUID(b []byte) (string, error) {
	if len(b) != 16 {
		return "", fmt.Errorf("uuid: expected 16 bytes, got %d", len(b))
	}
	const hexDigits = "0123456789abcdef"

	out := make([]byte, 36)
	pos := 0
	for i, v := range b {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out[pos] = '-'
			pos++
		}
		out[pos] = hexDigits[v>>4]
		out[pos+1] = hexDigits[v&0x0f]
		pos += 2
	}
	return string(out), nil
}

// decodeBinaryInterval decodes the 16-byte binary INTERVAL wire format:
// int64 microseconds, int32 days, int32 months.
func decodeBinaryInterval(b []byte) (PgInterval, error) {
	if len(b) != 16 {
		return PgInterval{}, fmt.Errorf("interval: expected 16 bytes, got %d", len(b))
	}
	return PgInterval{
		Microseconds: int64(binary.BigEndian.Uint64(b[0:8])),
		Days:         int32(binary.BigEndian.Uint32(b[8:12])),
		Months:       int32(binary.BigEndian.Uint32(b[12:16])),
	}, nil
}

// Vector represents a pgvector dense vector of IEEE 754 float32 numbers.
type Vector []float32

// String formats the vector into PostgreSQL's standard literal syntax: "[1.0,2.0,3.0]".
func (v Vector) String() string {
	return FormatVectorLiteral(v)
}

// DecodeBinaryVector decodes the PostgreSQL pgvector binary wire format:
// uint16 dim, uint16 unused (0), followed by dim * 4 bytes of IEEE 754 float32 values in big-endian order.
func DecodeBinaryVector(b []byte) (Vector, error) {
	if len(b) < 4 {
		return nil, fmt.Errorf("pgvector: header requires 4 bytes, got %d", len(b))
	}
	dim := int(binary.BigEndian.Uint16(b[0:2]))
	unused := binary.BigEndian.Uint16(b[2:4])
	if unused != 0 {
		return nil, fmt.Errorf("pgvector: expected unused word to be 0, got %d", unused)
	}
	expectedLen := 4 + dim*4
	if len(b) != expectedLen {
		return nil, fmt.Errorf("pgvector: declared dimension %d expects %d bytes, got %d", dim, expectedLen, len(b))
	}
	vec := make(Vector, dim)
	offset := 4
	for i := 0; i < dim; i++ {
		bits := binary.BigEndian.Uint32(b[offset : offset+4])
		vec[i] = math.Float32frombits(bits)
		offset += 4
	}
	return vec, nil
}

// EncodeBinaryVector encodes a float32 vector into PostgreSQL pgvector binary wire format.
func EncodeBinaryVector(vec []float32) []byte {
	dim := len(vec)
	buf := make([]byte, 4+dim*4)
	binary.BigEndian.PutUint16(buf[0:2], uint16(dim))
	binary.BigEndian.PutUint16(buf[2:4], 0)
	offset := 4
	for _, val := range vec {
		bits := math.Float32bits(val)
		binary.BigEndian.PutUint32(buf[offset:offset+4], bits)
		offset += 4
	}
	return buf
}

// FormatVectorLiteral converts a float32 vector into PostgreSQL's canonical text literal format: "[1.0,2.0,3.0]".
func FormatVectorLiteral(vec []float32) string {
	if len(vec) == 0 {
		return "[]"
	}
	var sb strings.Builder
	sb.WriteByte('[')
	for i, v := range vec {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(strconv.FormatFloat(float64(v), 'f', -1, 32))
	}
	sb.WriteByte(']')
	return sb.String()
}
