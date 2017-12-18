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

package coder

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

func TestEncodeDecodeVarUint64(t *testing.T) {
	tests := []struct {
		value  uint64
		length int
	}{
		{0, 1},
		{1, 1},
		{127, 1},
		{128, 2},
		{1000000, 3},
		{12345678901234, 7},
		{18446744073709551615, 10},
	}

	for _, test := range tests {
		var buf bytes.Buffer
		if err := EncodeVarUint64(test.value, &buf); err != nil {
			t.Fatalf("EncodeVarUint64(%v) failed: %v", test.value, err)
		}

		t.Logf("Encoded %v to %v", test.value, buf.Bytes())

		if len(buf.Bytes()) != test.length {
			t.Errorf("EncodeVarUint64(%v) = %v, want %v", test.value, len(buf.Bytes()), test.length)
		}

		actual, err := DecodeVarUint64(&buf)
		if err != nil {
			t.Fatalf("DecodeVarUint64(<%v>) failed: %v", test.value, err)
		}
		if actual != test.value {
			t.Errorf("DecodeVarUint64(<%v>) = %v, want %v", test.value, actual, test.value)
		}
	}
}

func TestEncodeDecodeVarInt(t *testing.T) {
	tests := []struct {
		value  int32
		length int
	}{
		{-2147483648, 5},
		{-1, 5},
		{0, 1},
		{1, 1},
		{127, 1},
		{128, 2},
		{1000000, 3},
	}

	for _, test := range tests {
		var buf bytes.Buffer

		if err := EncodeVarInt(test.value, &buf); err != nil {
			t.Fatalf("EncodeVarInt(%v) failed: %v", test.value, err)
		}

		t.Logf("Encoded %v to %v", test.value, buf.Bytes())

		if len(buf.Bytes()) != test.length {
			t.Errorf("EncodeVarInt(%v) = %v, want %v", test.value, len(buf.Bytes()), test.length)
		}

		actual, err := DecodeVarInt(&buf)
		if err != nil {
			t.Fatalf("DecodeVarInt(<%v>) failed: %v", test.value, err)
		}
		if actual != test.value {
			t.Errorf("DecodeVarInt(<%v>) = %v, want %v", test.value, actual, test.value)
		}
	}
}

func TestVarIntZ(t *testing.T) {
	tests := []interface{}{
		int(1),
		int(-1),
		int8(8),
		int8(-8),
		int16(16),
		int16(-16),
		int32(32),
		int32(-32),
		int64(64),
		int64(-64),
	}

	for _, v := range tests {
		typ := reflect.ValueOf(v).Type()

		data := encVarIntZ(v)
		result, err := decVarIntZ(typ, data)
		if err != nil {
			t.Fatalf("dec(enc(%v)) failed: %v", v, err)
		}

		if v != result {
			t.Errorf("dec(enc(%v)) = %v, want id", v, result)
		}
		resultT := reflectx.UnderlyingType(reflect.ValueOf(result)).Type()
		if resultT != typ {
			t.Errorf("type(dec(enc(%v))) = %v, want id", typ, resultT)
		}
	}
}

func TestVarUintZ(t *testing.T) {
	tests := []interface{}{
		uint(1),
		uint8(8),
		uint16(16),
		uint32(32),
		uint64(64),
	}

	for _, v := range tests {
		typ := reflect.ValueOf(v).Type()

		data := encVarUintZ(v)
		result, err := decVarUintZ(typ, data)
		if err != nil {
			t.Fatalf("dec(enc(%v)) failed: %v", v, err)
		}

		if v != result {
			t.Errorf("dec(enc(%v)) = %v, want id", v, result)
		}
		resultT := reflectx.UnderlyingType(reflect.ValueOf(result)).Type()
		if resultT != typ {
			t.Errorf("type(dec(enc(%v))) = %v, want id", typ, resultT)
		}
	}
}
