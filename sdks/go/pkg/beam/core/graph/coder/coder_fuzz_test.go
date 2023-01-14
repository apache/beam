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
	"math"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func FuzzEncodeDecodeBytes(f *testing.F) {
	f.Add([]byte{10})
	f.Fuzz(func(t *testing.T, b []byte) {
		var buf bytes.Buffer
		err := EncodeBytes(b, &buf)
		if err != nil {
			return
		}

		got, err := DecodeBytes(&buf)
		if err != nil {
			t.Fatalf("failed to decode bytes, got %v", err)
		}

		if d := cmp.Diff(got, b); d != "" {
			t.Errorf("decoded output does not match input: got %v, want %v", got, b)
		}
	})
}

const floatPrecision = float64(0.001)

func FuzzEncodeDecodeDouble(f *testing.F) {
	f.Add(float64(3.141))
	f.Fuzz(func(t *testing.T, a float64) {
		var buf bytes.Buffer
		err := EncodeDouble(a, &buf)
		if err != nil {
			return
		}

		actual, err := DecodeDouble(&buf)
		if err != nil {
			t.Fatalf("DecodeDouble(%v) failed: %v", &buf, err)
		}
		if math.Abs(actual-a) > floatPrecision {
			t.Fatalf("got %f, want %f +/- %f", actual, a, floatPrecision)
		}
	})
}

func FuzzEncodeDecodeSinglePrecisionFloat(f *testing.F) {
	f.Add(float32(3.141))
	f.Fuzz(func(t *testing.T, a float32) {
		var buf bytes.Buffer
		err := EncodeSinglePrecisionFloat(a, &buf)
		if err != nil {
			return
		}

		actual, err := DecodeSinglePrecisionFloat(&buf)
		if err != nil {
			t.Fatalf("DecodeDouble(%v) failed: %v", &buf, err)
		}
		if math.Abs(float64(actual-a)) > floatPrecision {
			t.Fatalf("got %f, want %f +/- %f", actual, a, floatPrecision)
		}
	})
}

func FuzzEncodeDecodeUInt64(f *testing.F) {
	f.Add(uint64(42))
	f.Fuzz(func(t *testing.T, b uint64) {
		var buf bytes.Buffer
		err := EncodeUint64(b, &buf)
		if err != nil {
			return
		}

		got, err := DecodeUint64(&buf)
		if err != nil {
			t.Fatalf("failed to decode bytes, got %v", err)
		}

		if got != b {
			t.Errorf("decoded output does not match input: got %v, want %v", got, b)
		}
	})
}

func FuzzEncodeDecodeInt32(f *testing.F) {
	f.Add(int32(42))
	f.Fuzz(func(t *testing.T, b int32) {
		var buf bytes.Buffer
		err := EncodeInt32(b, &buf)
		if err != nil {
			return
		}

		got, err := DecodeInt32(&buf)
		if err != nil {
			t.Fatalf("failed to decode bytes, got %v", err)
		}

		if got != b {
			t.Errorf("decoded output does not match input: got %v, want %v", got, b)
		}
	})
}

func FuzzEncodeDecodeStringUTF8LP(f *testing.F) {
	for _, s := range testValues {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, b string) {
		var build strings.Builder
		err := EncodeStringUTF8(b, &build)
		if err != nil {
			return
		}

		buf := bytes.NewBufferString(build.String())
		got, err := DecodeStringUTF8(buf)
		if err != nil {
			t.Fatalf("failed to decode bytes, got %v", err)
		}

		if got != b {
			t.Errorf("decoded output does not match input: got %v, want %v", got, b)
		}
	})
}
