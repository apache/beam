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
	"encoding/binary"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/testing/protocmp"
)

func atomicType(at pipepb.AtomicType) *pipepb.FieldType {
	return &pipepb.FieldType{TypeInfo: &pipepb.FieldType_AtomicType{AtomicType: at}}
}

func logicalFieldType(urn string, representation *pipepb.FieldType) *pipepb.FieldType {
	return &pipepb.FieldType{
		TypeInfo: &pipepb.FieldType_LogicalType{
			LogicalType: &pipepb.LogicalType{
				Urn:            urn,
				Representation: representation,
			},
		},
	}
}

// standardRow uses the standard logical types as fields, with the same field
// names as the micros_instant case of standard_coders.yaml.
type standardRow struct {
	F_timestamp MicrosInstant `beam:"f_timestamp"`
	F_string    string        `beam:"f_string"`
	F_int       int64         `beam:"f_int"`
	F_date      Date          `beam:"f_date"`
}

func TestStandardLogicalTypes_Schema(t *testing.T) {
	rt := reflect.TypeOf(standardRow{})
	want := &pipepb.Schema{
		Fields: []*pipepb.Field{
			{Name: "f_timestamp", Type: logicalFieldType(URNMicrosInstant, &pipepb.FieldType{
				TypeInfo: &pipepb.FieldType_RowType{RowType: &pipepb.RowType{Schema: &pipepb.Schema{
					Fields: []*pipepb.Field{
						{Name: "seconds", Type: atomicType(pipepb.AtomicType_INT64)},
						{Name: "micros", Type: atomicType(pipepb.AtomicType_INT64)},
					},
				}}},
			})},
			{Name: "f_string", Type: atomicType(pipepb.AtomicType_STRING)},
			{Name: "f_int", Type: atomicType(pipepb.AtomicType_INT64)},
			{Name: "f_date", Type: logicalFieldType(URNDate, atomicType(pipepb.AtomicType_INT64))},
		},
	}
	got, err := FromType(rt)
	if err != nil {
		t.Fatalf("FromType(%v) = %v, want nil error", rt, err)
	}
	if d := cmp.Diff(want, got, protocmp.Transform(), protocmp.IgnoreFields(&pipepb.Schema{}, "id")); d != "" {
		t.Errorf("FromType(%v) diff (-want, +got): %v", rt, d)
	}
	// A schema from another SDK has a different id, and must still map to the
	// standard Go types.
	want.Id = "standard-logical-types-test"
	gotType, err := ToType(want)
	if err != nil {
		t.Fatalf("ToType(%v) = %v, want nil error", prototext.Format(want), err)
	}
	for i, wantField := range []reflect.Type{typeOf[MicrosInstant](), typeOf[string](), typeOf[int64](), typeOf[Date]()} {
		if got := gotType.Field(i).Type; got != wantField {
			t.Errorf("ToType field %d type = %v, want %v", i, got, wantField)
		}
	}
}

func TestStandardLogicalTypes_RowEncoding(t *testing.T) {
	// The bytes are the micros_instant example of standard_coders.yaml, with a
	// trailing date field appended: 2020-08-13 is 18487 days since the epoch.
	want := standardRow{
		F_timestamp: MicrosInstant(time.Date(2020, 8, 13, 14, 14, 14, 123456000, time.UTC)),
		F_string:    "2020-08-13T14:14:14.123456Z",
		F_int:       1597328054123456,
		F_date:      Date{Year: 2020, Month: time.August, Day: 13},
	}
	wantBytes := []byte("\x04\x00\x02\x00\xb6\x95\xd5\xf9\x05\xc0\xc4\x07\x1b2020-08-13T14:14:14.123456Z\xc0\xf7\x85\xda\xae\x98\xeb\x02\xb7\x90\x01")
	rt := reflect.TypeOf(want)
	enc, err := coder.RowEncoderForStruct(rt)
	if err != nil {
		t.Fatalf("RowEncoderForStruct(%v) = %v, want nil error", rt, err)
	}
	var buf bytes.Buffer
	if err := enc(want, &buf); err != nil {
		t.Fatalf("enc(%v) = %v, want nil error", want, err)
	}
	if got := buf.Bytes(); !bytes.Equal(got, wantBytes) {
		t.Fatalf("enc(%v) = %q, want %q", want, got, wantBytes)
	}
	dec, err := coder.RowDecoderForStruct(rt)
	if err != nil {
		t.Fatalf("RowDecoderForStruct(%v) = %v, want nil error", rt, err)
	}
	got, err := dec(bytes.NewBuffer(wantBytes))
	if err != nil {
		t.Fatalf("dec(%q) = %v, want nil error", wantBytes, err)
	}
	if d := cmp.Diff(want, got, cmp.Comparer(func(a, b MicrosInstant) bool { return a.Time().Equal(b.Time()) })); d != "" {
		t.Errorf("dec(enc(%v)) diff (-want, +got): %v", want, d)
	}
}

func TestMicrosInstant_SubMicrosecondPrecision(t *testing.T) {
	enc, err := coder.RowEncoderForStruct(reflect.TypeOf(standardRow{}))
	if err != nil {
		t.Fatalf("RowEncoderForStruct = %v, want nil error", err)
	}
	row := standardRow{F_timestamp: MicrosInstant(time.Unix(0, 1500))}
	if err := enc(row, &bytes.Buffer{}); err == nil {
		t.Errorf("enc(%v) = nil error, want error for sub microsecond precision", row)
	}
}

func TestDate_Storage(t *testing.T) {
	tests := []struct {
		date Date
		days int64
	}{
		{Date{1970, time.January, 1}, 0},
		{Date{1969, time.December, 31}, -1},
		{Date{2020, time.August, 13}, 18487},
		{Date{1600, time.February, 29}, -135081},
		{Date{9999, time.December, 31}, 2932896},
	}
	for _, test := range tests {
		days, err := dateToStorage(test.date)
		if err != nil {
			t.Fatalf("dateToStorage(%v) = %v, want nil error", test.date, err)
		}
		if days != test.days {
			t.Errorf("dateToStorage(%v) = %v, want %v", test.date, days, test.days)
		}
		date, err := dateFromStorage(test.days)
		if err != nil {
			t.Fatalf("dateFromStorage(%v) = %v, want nil error", test.days, err)
		}
		if date != test.date {
			t.Errorf("dateFromStorage(%v) = %v, want %v", test.days, date, test.date)
		}
	}
	if got, want := DateOf(time.Date(2020, 8, 13, 23, 59, 59, 0, time.UTC)).String(), "2020-08-13"; got != want {
		t.Errorf("DateOf(...).String() = %v, want %v", got, want)
	}
}

func TestRegisterLogicalTypeConversion(t *testing.T) {
	type celsius float64
	type reading struct {
		Temperature celsius
	}
	// Registering with the default registry mirrors what users do in init().
	RegisterLogicalTypeConversion[celsius, float64]("schema.test:celsius:v1",
		func(c celsius) (float64, error) { return float64(c) + 273.15, nil },
		func(k float64) (celsius, error) { return celsius(k - 273.15), nil })

	rt := reflect.TypeOf(reading{})
	schm, err := FromType(rt)
	if err != nil {
		t.Fatalf("FromType(%v) = %v, want nil error", rt, err)
	}
	wantField := logicalFieldType("schema.test:celsius:v1", atomicType(pipepb.AtomicType_DOUBLE))
	if d := cmp.Diff(wantField, schm.GetFields()[0].GetType(), protocmp.Transform()); d != "" {
		t.Errorf("FromType(%v) field diff (-want, +got): %v", rt, d)
	}

	enc, err := coder.RowEncoderForStruct(rt)
	if err != nil {
		t.Fatalf("RowEncoderForStruct(%v) = %v, want nil error", rt, err)
	}
	var buf bytes.Buffer
	if err := enc(reading{Temperature: 10}, &buf); err != nil {
		t.Fatalf("enc = %v, want nil error", err)
	}
	// 283.15 as a big endian double, after the row header.
	wantBytes := []byte{0x01, 0x00, 0x40, 0x71, 0xb2, 0x66, 0x66, 0x66, 0x66, 0x66}
	if got := buf.Bytes(); !bytes.Equal(got, wantBytes) {
		t.Fatalf("enc(reading{10}) = %v, want %v", got, wantBytes)
	}
	dec, err := coder.RowDecoderForStruct(rt)
	if err != nil {
		t.Fatalf("RowDecoderForStruct(%v) = %v, want nil error", rt, err)
	}
	got, err := dec(&buf)
	if err != nil {
		t.Fatalf("dec = %v, want nil error", err)
	}
	if d := cmp.Diff(reading{Temperature: 10}, got, cmp.Comparer(func(a, b celsius) bool { return a-b < 1e-9 && b-a < 1e-9 })); d != "" {
		t.Errorf("dec(enc(reading{10})) diff (-want, +got): %v", d)
	}
}

// fixedCode is a user logical type with an argument, registered with the
// exported parameterized registration.
type fixedCode string

func TestRegisterParameterizedLogicalTypeConversion(t *testing.T) {
	RegisterParameterizedLogicalTypeConversion[fixedCode, string]("schema.test:fixed_code:v1", int32(4),
		func(c fixedCode) (string, error) { return string(c), nil },
		func(s string) (fixedCode, error) { return fixedCode(s), nil })
	rt := reflect.TypeOf(struct{ Code fixedCode }{})
	schm, err := FromType(rt)
	if err != nil {
		t.Fatalf("FromType(%v) = %v, want nil error", rt, err)
	}
	want := logicalFieldType("schema.test:fixed_code:v1", atomicType(pipepb.AtomicType_STRING))
	want.GetLogicalType().ArgumentType = int32FieldType()
	want.GetLogicalType().Argument = int32FieldValue(4)
	if d := cmp.Diff(want, schm.GetFields()[0].GetType(), protocmp.Transform()); d != "" {
		t.Errorf("FromType(%v) field diff (-want, +got): %v", rt, d)
	}
	schm.Id = "fixed-code-from-elsewhere"
	got, err := ToType(schm)
	if err != nil {
		t.Fatalf("ToType = %v, want nil error", err)
	}
	if ft := got.Field(0).Type; ft != typeOf[fixedCode]() {
		t.Errorf("ToType field type = %v, want %v", ft, typeOf[fixedCode]())
	}
	enc, err := coder.RowEncoderForStruct(rt)
	if err != nil {
		t.Fatalf("RowEncoderForStruct(%v) = %v, want nil error", rt, err)
	}
	var buf bytes.Buffer
	if err := enc(struct{ Code fixedCode }{"ABCD"}, &buf); err != nil {
		t.Fatalf("enc = %v, want nil error", err)
	}
	if got, want := buf.Bytes(), []byte("\x01\x00\x04ABCD"); !bytes.Equal(got, want) {
		t.Errorf("enc = %q, want %q", got, want)
	}
}

// money is a user logical type with its own wire encoding: the cents as a
// big endian int32 inside a BYTES representation.
type money int32

func TestRegisterLogicalTypeCoder(t *testing.T) {
	RegisterLogicalTypeCoder[money, []byte]("schema.test:money:v1",
		func(m money, w io.Writer) error {
			var b [4]byte
			binary.BigEndian.PutUint32(b[:], uint32(m))
			return coder.EncodeBytes(b[:], w)
		},
		func(r io.Reader) (money, error) {
			b, err := coder.DecodeBytes(r)
			if err != nil {
				return 0, err
			}
			return money(int32(binary.BigEndian.Uint32(b))), nil
		})
	rt := reflect.TypeOf(struct{ Price money }{})
	schm, err := FromType(rt)
	if err != nil {
		t.Fatalf("FromType(%v) = %v, want nil error", rt, err)
	}
	want := logicalFieldType("schema.test:money:v1", atomicType(pipepb.AtomicType_BYTES))
	if d := cmp.Diff(want, schm.GetFields()[0].GetType(), protocmp.Transform()); d != "" {
		t.Errorf("FromType(%v) field diff (-want, +got): %v", rt, d)
	}
	enc, err := coder.RowEncoderForStruct(rt)
	if err != nil {
		t.Fatalf("RowEncoderForStruct(%v) = %v, want nil error", rt, err)
	}
	var buf bytes.Buffer
	if err := enc(struct{ Price money }{-2}, &buf); err != nil {
		t.Fatalf("enc = %v, want nil error", err)
	}
	wantBytes := []byte{0x01, 0x00, 0x04, 0xff, 0xff, 0xff, 0xfe}
	if got := buf.Bytes(); !bytes.Equal(got, wantBytes) {
		t.Fatalf("enc = %v, want %v", got, wantBytes)
	}
	dec, err := coder.RowDecoderForStruct(rt)
	if err != nil {
		t.Fatalf("RowDecoderForStruct(%v) = %v, want nil error", rt, err)
	}
	got, err := dec(&buf)
	if err != nil {
		t.Fatalf("dec = %v, want nil error", err)
	}
	if d := cmp.Diff(struct{ Price money }{-2}, got); d != "" {
		t.Errorf("dec(enc) diff (-want, +got): %v", d)
	}
}

func TestRegisterLogicalTypeConversion_PointerStorage(t *testing.T) {
	type celsiusPtr float64
	defer func() {
		if recover() == nil {
			t.Errorf("RegisterLogicalTypeConversion with a pointer storage type did not panic")
		}
	}()
	RegisterLogicalTypeConversion[celsiusPtr, *float64]("schema.test:celsius_ptr:v1",
		func(c celsiusPtr) (*float64, error) { f := float64(c); return &f, nil },
		func(f *float64) (celsiusPtr, error) { return celsiusPtr(*f), nil })
}
