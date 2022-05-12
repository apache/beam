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
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestEncodeDecodeBytes(t *testing.T) {
	longString := strings.Repeat(" this sentence is 32 characters.", 8) // 256 characters to ensure LP works.
	tests := []struct {
		v       []byte
		encoded []byte
	}{
		{v: []byte{}, encoded: []byte{0}},
		{v: []byte{42}, encoded: []byte{1, 42}},
		{v: []byte{42, 23}, encoded: []byte{2, 42, 23}},
		{v: []byte(longString), encoded: append([]byte{128, 2}, []byte(longString)...)},
	}
	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("encode %q", test.v), func(t *testing.T) {
			var buf bytes.Buffer
			err := EncodeBytes(test.v, &buf)
			if err != nil {
				t.Fatalf("EncodeBytes(%q) = %v", test.v, err)
			}
			if d := cmp.Diff(test.encoded, buf.Bytes()); d != "" {
				t.Errorf("EncodeBytes(%q) = %v, want %v diff(-want,+got):\n %v", test.v, buf.Bytes(), test.encoded, d)
			}
		})
		t.Run(fmt.Sprintf("decode %v", test.v), func(t *testing.T) {
			buf := bytes.NewBuffer(test.encoded)
			got, err := DecodeBytes(buf)
			if err != nil {
				t.Fatalf("DecodeBytes(%q) = %v", test.encoded, err)
			}
			if d := cmp.Diff(test.v, got); d != "" {
				t.Errorf("DecodeBytes(%q) = %q, want %v diff(-want,+got):\n %v", test.encoded, got, test.v, d)
			}
		})
	}
}
