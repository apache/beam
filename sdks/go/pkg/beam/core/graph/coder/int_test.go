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
	"testing"
)

func TestEncodeDecodeUint64(t *testing.T) {
	tests := []uint64{
		0,
		1,
		127,
		128,
		1000000,
		12345678901234,
		18446744073709551615,
	}

	for _, test := range tests {
		var buf bytes.Buffer
		if err := EncodeUint64(test, &buf); err != nil {
			t.Fatalf("EncodeUint64(%v) failed: %v", test, err)
		}

		t.Logf("Encoded %v to %v", test, buf.Bytes())

		if len(buf.Bytes()) != 8 {
			t.Errorf("len(EncodeUint64(%v)) = %v, want 8", test, len(buf.Bytes()))
		}

		actual, err := DecodeUint64(&buf)
		if err != nil {
			t.Fatalf("DecodeUint64(<%v>) failed: %v", test, err)
		}
		if actual != test {
			t.Errorf("DecodeUint64(<%v>) = %v, want %v", test, actual, test)
		}
	}
}

func TestEncodeDecodeInt32(t *testing.T) {
	tests := []int32{
		-2147483648,
		-100000,
		-1,
		0,
		1,
		127,
		128,
		1000000,
	}

	for _, test := range tests {
		var buf bytes.Buffer
		if err := EncodeInt32(test, &buf); err != nil {
			t.Fatalf("EncodeInt32(%v) failed: %v", test, err)
		}

		t.Logf("Encoded %v to %v", test, buf.Bytes())

		if len(buf.Bytes()) != 4 {
			t.Errorf("len(EncodeInt32(%v)) = %v, want 4", test, len(buf.Bytes()))
		}

		actual, err := DecodeInt32(&buf)
		if err != nil {
			t.Fatalf("DecodeInt32(<%v>) failed: %v", test, err)
		}
		if actual != test {
			t.Errorf("DecodeInt32(<%v>) = %v, want %v", test, actual, test)
		}
	}
}

func TestEncodeDecodeInt16(t *testing.T) {
	tests := []struct {
		value int16
		want  []byte
	}{
		{-32768, []byte{0x80, 0x00}},
		{-2, []byte{0xff, 0xfe}},
		{-1, []byte{0xff, 0xff}},
		{0, []byte{0x00, 0x00}},
		{1, []byte{0x00, 0x01}},
		{999, []byte{0x03, 0xe7}},
		{32767, []byte{0x7f, 0xff}},
	}

	for _, test := range tests {
		var buf bytes.Buffer
		if err := EncodeInt16(test.value, &buf); err != nil {
			t.Fatalf("EncodeInt16(%v) failed: %v", test.value, err)
		}
		if got := buf.Bytes(); !bytes.Equal(got, test.want) {
			t.Errorf("EncodeInt16(%v) = %v, want %v", test.value, got, test.want)
		}
		actual, err := DecodeInt16(&buf)
		if err != nil {
			t.Fatalf("DecodeInt16(<%v>) failed: %v", test.value, err)
		}
		if actual != test.value {
			t.Errorf("DecodeInt16(<%v>) = %v, want %v", test.value, actual, test.value)
		}
	}
}
