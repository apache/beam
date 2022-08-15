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
	"testing"
)

func TestEncodeDecodeSinglePrecisionFloat(t *testing.T) {
	var tests []float32
	for x := float32(-100.0); x <= float32(100.0); x++ {
		tests = append(tests, 0.1*x)
	}
	tests = append(tests, -math.MaxFloat32)
	tests = append(tests, math.MaxFloat32)
	for _, test := range tests {
		var buf bytes.Buffer
		if err := EncodeSinglePrecisionFloat(test, &buf); err != nil {
			t.Fatalf("EncodeSinglePrecisionFloat(%v) failed: %v", test, err)
		}
		t.Logf("Encoded %v to %v", test, buf.Bytes())

		if len(buf.Bytes()) != 4 {
			t.Errorf("len(EncodeSinglePrecisionFloat(%v)) = %v, want 4", test, len(buf.Bytes()))
		}

		actual, err := DecodeSinglePrecisionFloat(&buf)
		if err != nil {
			t.Fatalf("DecodeSinglePrecisionFloat(<%v>) failed: %v", test, err)
		}
		if actual != test {
			t.Errorf("DecodeSinglePrecisionFloat(<%v>) = %v, want %v", test, actual, test)
		}
	}
}
