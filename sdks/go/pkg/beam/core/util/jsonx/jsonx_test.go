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

package jsonx

import (
	"encoding/json"
	"math"
	"testing"
)

type someStruct struct {
	SomeNumber  int64
	SomeString  string
	SomeBoolean bool
}

func (s *someStruct) equals(other *someStruct) bool {
	return (s.SomeNumber == other.SomeNumber) && (s.SomeString == other.SomeString) && (s.SomeBoolean == other.SomeBoolean)
}

func TestEncodeDecode(t *testing.T) {
	var tests = []struct {
		name        string
		inputNum    int64
		inputString string
		inputBool   bool
	}{
		{
			"simple inputs",
			10,
			"information",
			true,
		},
		{
			"default inputs",
			0,
			"",
			false,
		},
		{
			"maximum int",
			math.MaxInt64,
			"big int",
			true,
		},
		{
			"minimum int",
			math.MinInt64,
			"little int",
			false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inputStruct := &someStruct{SomeNumber: test.inputNum, SomeString: test.inputString, SomeBoolean: test.inputBool}
			enc, err := Marshal(inputStruct)
			if err != nil {
				t.Fatalf("Marshal(%v) failed, got %v", inputStruct, err)
			}
			if !json.Valid(enc) {
				t.Errorf("encoding is not valid JSON, got %v", enc)
			}
			outputStruct := &someStruct{}
			err = Unmarshal(outputStruct, enc)
			if err != nil {
				t.Fatalf("Unmarshal() failed, got %v", err)
			}
			if !inputStruct.equals(outputStruct) {
				t.Errorf("input and output mismatch, got %v, want %v", outputStruct, inputStruct)
			}
		})
	}
}
