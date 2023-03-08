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

package ioutilx

import (
	"io"
	"strings"
	"testing"
)

func TestReadN(t *testing.T) {
	testString := "hello world!"
	testLength := len(testString)
	r := strings.NewReader(testString)

	// test good read
	data, err := ReadN(r, testLength)
	// err is expected to be nil
	if err != nil {
		t.Errorf("failed to read data, got error: %v", err)
	}
	// length of data is expected to match testLength
	if got, want := len(data), testLength; got != want {
		t.Errorf("got length %v, wanted %v", got, want)
	}
	// content of data is expected to match testString
	if got, want := string(data), testString; got != want {
		t.Errorf("got string %q, wanted %q", got, want)
	}
}

func TestReadN_Bad(t *testing.T) {
	testString := "hello world!"
	testLength := len(testString)
	r := strings.NewReader(testString)

	// test bad read
	_, err := ReadN(r, testLength+1)
	// err is expected to be io.EOF
	if err != io.EOF {
		t.Errorf("got error: %v\nwanted error: %v", err, io.EOF)
	}
}

func TestReadNBufUnsafe(t *testing.T) {
	testString := "hello world!"
	testLength := len(testString)
	r := strings.NewReader(testString)
	data := make([]byte, testLength)

	err := ReadNBufUnsafe(r, data)
	// err is expected to be nil
	if err != nil {
		t.Errorf("failed to read data, got error: %v", err)
	}
	// content of data is expected to match testString
	if got, want := string(data), testString; got != want {
		t.Errorf("got string %q, wanted %q", got, want)
	}
}

func TestReadUnsafe(t *testing.T) {
	testString := "hello world!"
	testLength := len(testString)
	r := strings.NewReader(testString)
	data := make([]byte, testLength)

	length, err := ReadUnsafe(r, data)
	// err is expected to be nil
	if err != nil {
		t.Errorf("failed to read data, got error: %v", err)
	}
	// length of data is expected to match testLength
	if length != testLength {
		t.Errorf("got length %v, wanted %v", length, testLength)
	}
	// content of data is expected to match testString
	if got, want := string(data), testString; got != want {
		t.Errorf("got string %q, wanted %q", got, want)
	}
}
