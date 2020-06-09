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
	"encoding/base64"
	"io"
	"strings"
	"testing"
	"unicode/utf8"
)

var testValues = []string{
	"",
	"a",
	"13",
	"hello",
	"a longer string with spaces and all that",
	"a string with a \n newline",
	"スタリング",
	"I am the very model of a modern major general.\nI've information animal, vegetable, and mineral",
}

// Base64 encoded versions of the above strings, without the length prefix.
var testEncodings = []string{
	"",
	"YQ",
	"MTM",
	"aGVsbG8",
	"YSBsb25nZXIgc3RyaW5nIHdpdGggc3BhY2VzIGFuZCBhbGwgdGhhdA",
	"YSBzdHJpbmcgd2l0aCBhIAogbmV3bGluZQ",
	"44K544K_44Oq44Oz44Kw",
	"SSBhbSB0aGUgdmVyeSBtb2RlbCBvZiBhIG1vZGVybiBtYWpvciBnZW5lcmFsLgpJJ3ZlIGluZm9ybWF0aW9uIGFuaW1hbCwgdmVnZXRhYmxlLCBhbmQgbWluZXJhbA",
}

// TestLen serves as a verification that string lengths
// match their equivalent byte lengths, and not their rune
// representation.
func TestLen(t *testing.T) {
	runeCount := []int{0, 1, 2, 5, 40, 25, 5, 94}
	for i, s := range testValues {
		if got, want := len(s), len([]byte(s)); got != want {
			t.Errorf("string and []byte len do not match. got %v, want %v", got, want)
		}
		if got, want := utf8.RuneCountInString(s), runeCount[i]; got != want {
			t.Errorf("Rune count of %q change len do not match. got %v, want %v", s, got, want)
		}
	}
}

func TestEncodeStringUTF8(t *testing.T) {
	for i, s := range testValues {
		s := s
		want := testEncodings[i]
		t.Run(s, func(t *testing.T) {
			var b strings.Builder
			base64enc := base64.NewEncoder(base64.RawURLEncoding, &b)

			if err := encodeStringUTF8(s, base64enc); err != nil {
				t.Fatal(err)
			}
			base64enc.Close()
			got := b.String()
			if got != want {
				t.Errorf("encodeStringUTF8(%q) = %q, want %q", s, got, want)
			}
		})
	}
}

func TestDecodeStringUTF8(t *testing.T) {
	for i, s := range testEncodings {
		s := s
		want := testValues[i]
		t.Run(want, func(t *testing.T) {
			b := bytes.NewBufferString(s)
			base64dec := base64.NewDecoder(base64.RawURLEncoding, b)

			got, err := decodeStringUTF8(int64(len(want)), base64dec)
			if err != nil && err != io.EOF {
				t.Fatal(err)
			}
			if got != want {
				t.Errorf("decodeStringUTF8(%q) = %q, want %q", s, got, want)
			}
		})
	}
}

func TestEncodeDecodeStringUTF8LP(t *testing.T) {
	for _, s := range testValues {
		want := s
		t.Run(want, func(t *testing.T) {
			var build strings.Builder
			if err := EncodeStringUTF8(want, &build); err != nil {
				t.Fatal(err)
			}
			buf := bytes.NewBufferString(build.String())
			got, err := DecodeStringUTF8(buf)
			if err != nil && err != io.EOF {
				t.Fatal(err)
			}
			if got != want {
				t.Errorf("decodeStringUTF8(%q) = %q, want %q", s, got, want)
			}
		})
	}
}
