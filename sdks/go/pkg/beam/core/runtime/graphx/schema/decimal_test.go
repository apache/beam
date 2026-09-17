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

package schema

import (
	"bytes"
	"math/big"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/google/go-cmp/cmp"
)

func decimalOf(unscaled string, scale int32) Decimal {
	u, ok := new(big.Int).SetString(unscaled, 10)
	if !ok {
		panic("invalid unscaled value " + unscaled)
	}
	return Decimal{Unscaled: u, Scale: scale}
}

func decimalEqual(a, b Decimal) bool {
	zero := new(big.Int)
	ua, ub := a.Unscaled, b.Unscaled
	if ua == nil {
		ua = zero
	}
	if ub == nil {
		ub = zero
	}
	return a.Scale == b.Scale && ua.Cmp(ub) == 0
}

func TestDecimal_StringParse(t *testing.T) {
	tests := []struct {
		decimal Decimal
		text    string
	}{
		{decimalOf("0", 1), "0.0"},
		{decimalOf("10", 1), "1.0"},
		{decimalOf("31415", 4), "3.1415"},
		{decimalOf("-100123", 3), "-100.123"},
		{decimalOf("5", 3), "0.005"},
		{decimalOf("-5", 3), "-0.005"},
		{decimalOf("42", 0), "42"},
		{decimalOf("1180591620717411303424", 0), "1180591620717411303424"},
	}
	for _, test := range tests {
		if got := test.decimal.String(); got != test.text {
			t.Errorf("%v.String() = %q, want %q", test.decimal, got, test.text)
		}
		got, err := ParseDecimal(test.text)
		if err != nil {
			t.Fatalf("ParseDecimal(%q) = %v, want nil error", test.text, err)
		}
		if !decimalEqual(got, test.decimal) {
			t.Errorf("ParseDecimal(%q) = %v, want %v", test.text, got, test.decimal)
		}
	}
	if got, want := (Decimal{Unscaled: big.NewInt(42), Scale: -3}).String(), "42000"; got != want {
		t.Errorf("negative scale String() = %q, want %q", got, want)
	}
	if got, want := (Decimal{}).String(), "0"; got != want {
		t.Errorf("zero value String() = %q, want %q", got, want)
	}
	if got, err := ParseDecimal("+7.5"); err != nil || !decimalEqual(got, decimalOf("75", 1)) {
		t.Errorf("ParseDecimal(\"+7.5\") = %v, %v, want 7.5", got, err)
	}
	for _, text := range []string{"", "-", ".", "1e3", "abc", "1.2.3", "1_000"} {
		if got, err := ParseDecimal(text); err == nil {
			t.Errorf("ParseDecimal(%q) = %v, want error", text, got)
		}
	}
}

func TestTwosComplementBytes(t *testing.T) {
	tests := []struct {
		value string
		want  []byte
	}{
		{"0", []byte{0x00}},
		{"1", []byte{0x01}},
		{"127", []byte{0x7f}},
		{"128", []byte{0x00, 0x80}},
		{"255", []byte{0x00, 0xff}},
		{"256", []byte{0x01, 0x00}},
		{"-1", []byte{0xff}},
		{"-128", []byte{0x80}},
		{"-129", []byte{0xff, 0x7f}},
		{"-256", []byte{0xff, 0x00}},
		{"-100123", []byte{0xfe, 0x78, 0xe5}},
		{"31415", []byte{0x7a, 0xb7}},
	}
	for _, test := range tests {
		x, _ := new(big.Int).SetString(test.value, 10)
		got := twosComplementBytes(x)
		if !bytes.Equal(got, test.want) {
			t.Errorf("twosComplementBytes(%v) = %x, want %x", test.value, got, test.want)
		}
		if back := bigIntFromTwosComplement(test.want); back.Cmp(x) != 0 {
			t.Errorf("bigIntFromTwosComplement(%x) = %v, want %v", test.want, back, test.value)
		}
	}
	if got := twosComplementBytes(nil); !bytes.Equal(got, []byte{0x00}) {
		t.Errorf("twosComplementBytes(nil) = %x, want 00", got)
	}
}

// decimalRow has the fields of the decimal case of standard_coders.yaml.
type decimalRow struct {
	F_float   float32 `beam:"f_float"`
	F_decimal Decimal `beam:"f_decimal"`
}

func TestDecimal_RowEncoding(t *testing.T) {
	// The bytes are the examples of the decimal case of standard_coders.yaml.
	tests := []struct {
		row       decimalRow
		wantBytes []byte
	}{
		{decimalRow{0, decimalOf("0", 1)}, []byte("\x02\x00\x00\x00\x00\x00\x01\x01\x00")},
		{decimalRow{1, decimalOf("10", 1)}, []byte("\x02\x00?\x80\x00\x00\x01\x01\n")},
		{decimalRow{3.1415, decimalOf("31415", 4)}, []byte("\x02\x00@I\x0eV\x04\x02z\xb7")},
		{decimalRow{-100.123, decimalOf("-100123", 3)}, []byte("\x02\x00\xc2\xc8>\xfa\x03\x03\xfex\xe5")},
	}
	rt := reflect.TypeOf(decimalRow{})
	enc, err := coder.RowEncoderForStruct(rt)
	if err != nil {
		t.Fatalf("RowEncoderForStruct(%v) = %v, want nil error", rt, err)
	}
	dec, err := coder.RowDecoderForStruct(rt)
	if err != nil {
		t.Fatalf("RowDecoderForStruct(%v) = %v, want nil error", rt, err)
	}
	for _, test := range tests {
		var buf bytes.Buffer
		if err := enc(test.row, &buf); err != nil {
			t.Fatalf("enc(%v) = %v, want nil error", test.row, err)
		}
		if got := buf.Bytes(); !bytes.Equal(got, test.wantBytes) {
			t.Errorf("enc(%v) = %q, want %q", test.row, got, test.wantBytes)
		}
		got, err := dec(bytes.NewBuffer(test.wantBytes))
		if err != nil {
			t.Fatalf("dec(%q) = %v, want nil error", test.wantBytes, err)
		}
		if d := cmp.Diff(test.row, got, cmp.Comparer(decimalEqual)); d != "" {
			t.Errorf("dec(%q) diff (-want, +got): %v", test.wantBytes, d)
		}
	}
	// The schema of a Decimal field is the decimal logical type with a BYTES representation.
	schm, err := FromType(rt)
	if err != nil {
		t.Fatalf("FromType(%v) = %v, want nil error", rt, err)
	}
	lt := schm.GetFields()[1].GetType().GetLogicalType()
	if lt.GetUrn() != URNDecimal || lt.GetRepresentation().GetAtomicType().String() != "BYTES" {
		t.Errorf("FromType(%v) decimal field = %v, want %v with BYTES representation", rt, lt, URNDecimal)
	}
}
