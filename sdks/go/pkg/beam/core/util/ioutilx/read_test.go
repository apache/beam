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
	const testString = "hello world!"
	const testLength = len(testString)
	r := strings.NewReader(testString)

	// test good read
	data, err := ReadN(r, testLength)
	// err is expected to be nil
	if err != nil {
		t.Errorf("Failed to read data: %v", err)
	}
	// length of data is expected to match testLength
	if (len(data) != testLength) {
		t.Errorf("Got %v, wanted %v", len(data), testLength)
	}
	// content of data is expected to match testString
	if (string(data) != testString) {
		t.Errorf("Got %q, wanted %q", string(data), testString)
	}

	// test bad read
	data, err = ReadN(r, testLength+1)
	// err is expected to be io.EOF
	if err != io.EOF {
		t.Errorf("Got %v, wanted %v", err, io.EOF)
	}
}

func TestReadNBufUnsafe(t *testing.T) {
	const testString = "hello world!"
	const testLength = len(testString)
	r := strings.NewReader(testString)
	var data [testLength]byte

	err := ReadNBufUnsafe(r, data[:])
	// err is expected to be nil
	if err != nil {
		t.Errorf("Failed to read data: %v", err)
	}
	// content of data is expected to match testString
	if (string(data[:]) != testString) {
		t.Errorf("Got %q, wanted %q", string(data[:]), testString)
	}
}

func TestReadUnsafe(t *testing.T) {
	const testString = "hello world!"
	const testLength = len(testString)
	r := strings.NewReader(testString)
	var data [testLength]byte

	length, err := ReadUnsafe(r, data[:])
	// err is expected to be nil
	if err != nil {
		t.Errorf("Failed to read data: %v", err)
	}
	// length of data is expected to match testLength
	if (length != testLength) {
		t.Errorf("Got %v, wanted %v", length, testLength)
	}
	// content of data is expected to match testString
	if (string(data[:]) != testString) {
		t.Errorf("Got %q, wanted %q", string(data[:]), testString)
	}
}
