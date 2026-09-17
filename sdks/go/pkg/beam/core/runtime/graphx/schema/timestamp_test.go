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
	"reflect"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

type millisTimestampRow struct {
	F_timestamp TimestampMillis `beam:"f_timestamp"`
}

type microsTimestampRow struct {
	F_timestamp TimestampMicros `beam:"f_timestamp"`
}

type nanosTimestampRow struct {
	F_timestamp TimestampNanos `beam:"f_timestamp"`
}

func timestampSchema(subseconds pipepb.AtomicType, precision int32) *pipepb.Schema {
	ft := logicalFieldType(URNTimestamp, &pipepb.FieldType{
		TypeInfo: &pipepb.FieldType_RowType{RowType: &pipepb.RowType{Schema: &pipepb.Schema{
			Fields: []*pipepb.Field{
				{Name: "seconds", Type: atomicType(pipepb.AtomicType_INT64)},
				{Name: "subseconds", Type: atomicType(subseconds)},
			},
		}}},
	})
	ft.GetLogicalType().ArgumentType = int32FieldType()
	ft.GetLogicalType().Argument = int32FieldValue(precision)
	return &pipepb.Schema{Fields: []*pipepb.Field{{Name: "f_timestamp", Type: ft}}}
}

func TestTimestamp_Schema(t *testing.T) {
	tests := []struct {
		rt         reflect.Type
		subseconds pipepb.AtomicType
		precision  int32
	}{
		{reflect.TypeOf(millisTimestampRow{}), pipepb.AtomicType_INT16, 3},
		{reflect.TypeOf(microsTimestampRow{}), pipepb.AtomicType_INT32, 6},
		{reflect.TypeOf(nanosTimestampRow{}), pipepb.AtomicType_INT32, 9},
	}
	for _, test := range tests {
		want := timestampSchema(test.subseconds, test.precision)
		got, err := FromType(test.rt)
		if err != nil {
			t.Fatalf("FromType(%v) = %v, want nil error", test.rt, err)
		}
		if d := cmp.Diff(want, got, protocmp.Transform(), protocmp.IgnoreFields(&pipepb.Schema{}, "id")); d != "" {
			t.Errorf("FromType(%v) diff (-want, +got): %v", test.rt, d)
		}
		want.Id = test.rt.String()
		gotType, err := ToType(want)
		if err != nil {
			t.Fatalf("ToType(precision %d) = %v, want nil error", test.precision, err)
		}
		if got, wantField := gotType.Field(0).Type, test.rt.Field(0).Type; got != wantField {
			t.Errorf("ToType(precision %d) field type = %v, want exactly %v", test.precision, got, wantField)
		}
	}
	unsupported := timestampSchema(pipepb.AtomicType_INT16, 4)
	unsupported.Id = "timestamp-precision-4"
	if got, err := ToType(unsupported); err == nil {
		t.Errorf("ToType(precision 4) = %v, want error", got)
	}
}

func TestTimestamp_RowEncoding(t *testing.T) {
	// The bytes are the timestamp:v1 examples of standard_coders.yaml.
	tests := []struct {
		row       any
		wantBytes []byte
	}{
		{millisTimestampRow{TimestampMillis(time.Unix(1597328054, 999000000).UTC())}, []byte("\x01\x00\x02\x00\xb6\x95\xd5\xf9\x05\x03\xe7")},
		{millisTimestampRow{TimestampMillis(time.Unix(-2, 500000000).UTC())}, []byte("\x01\x00\x02\x00\xfe\xff\xff\xff\xff\xff\xff\xff\xff\x01\x01\xf4")},
		{millisTimestampRow{TimestampMillis(time.Unix(1597328054, 0).UTC())}, []byte("\x01\x00\x02\x00\xb6\x95\xd5\xf9\x05\x00\x00")},
		{microsTimestampRow{TimestampMicros(time.Unix(1597328054, 123456000).UTC())}, []byte("\x01\x00\x02\x00\xb6\x95\xd5\xf9\x05\xc0\xc4\x07")},
		{microsTimestampRow{TimestampMicros(time.Unix(-2, 500000000).UTC())}, []byte("\x01\x00\x02\x00\xfe\xff\xff\xff\xff\xff\xff\xff\xff\x01\xa0\xc2\x1e")},
		{nanosTimestampRow{TimestampNanos(time.Unix(1597328054, 123456789).UTC())}, []byte("\x01\x00\x02\x00\xb6\x95\xd5\xf9\x05\x95\x9a\xef:")},
		{nanosTimestampRow{TimestampNanos(time.Unix(-2, 999999999).UTC())}, []byte("\x01\x00\x02\x00\xfe\xff\xff\xff\xff\xff\xff\xff\xff\x01\xff\x93\xeb\xdc\x03")},
	}
	timeEqual := cmp.Options{
		cmp.Comparer(func(a, b TimestampMillis) bool { return a.Time().Equal(b.Time()) }),
		cmp.Comparer(func(a, b TimestampMicros) bool { return a.Time().Equal(b.Time()) }),
		cmp.Comparer(func(a, b TimestampNanos) bool { return a.Time().Equal(b.Time()) }),
	}
	for _, test := range tests {
		rt := reflect.TypeOf(test.row)
		enc, err := coder.RowEncoderForStruct(rt)
		if err != nil {
			t.Fatalf("RowEncoderForStruct(%v) = %v, want nil error", rt, err)
		}
		var buf bytes.Buffer
		if err := enc(test.row, &buf); err != nil {
			t.Fatalf("enc(%v) = %v, want nil error", test.row, err)
		}
		if got := buf.Bytes(); !bytes.Equal(got, test.wantBytes) {
			t.Errorf("enc(%v) = %q, want %q", test.row, got, test.wantBytes)
		}
		dec, err := coder.RowDecoderForStruct(rt)
		if err != nil {
			t.Fatalf("RowDecoderForStruct(%v) = %v, want nil error", rt, err)
		}
		got, err := dec(bytes.NewBuffer(test.wantBytes))
		if err != nil {
			t.Fatalf("dec(%q) = %v, want nil error", test.wantBytes, err)
		}
		if d := cmp.Diff(test.row, got, timeEqual); d != "" {
			t.Errorf("dec(%q) diff (-want, +got): %v", test.wantBytes, d)
		}
	}
}

func TestTimestamp_Errors(t *testing.T) {
	enc, err := coder.RowEncoderForStruct(reflect.TypeOf(millisTimestampRow{}))
	if err != nil {
		t.Fatalf("RowEncoderForStruct = %v, want nil error", err)
	}
	row := millisTimestampRow{TimestampMillis(time.Unix(0, 1500000))}
	if err := enc(row, &bytes.Buffer{}); err == nil {
		t.Errorf("enc(%v) = nil error, want error for sub millisecond precision", row)
	}
	if _, err := timestampMillisFromStorage(timestampShortStorage{Seconds: 0, Subseconds: 1000}); err == nil {
		t.Errorf("timestampMillisFromStorage(subseconds 1000) = nil error, want error")
	}
	if _, err := timestampNanosFromStorage(timestampStorage{Seconds: 0, Subseconds: -1}); err == nil {
		t.Errorf("timestampNanosFromStorage(subseconds -1) = nil error, want error")
	}
}
