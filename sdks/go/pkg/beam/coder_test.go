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

package beam

import (
	"bytes"
	"reflect"
	"testing"
)

func TestJSONCoder(t *testing.T) {
	v := "teststring"
	tests := []interface{}{
		43,
		12431235,
		-2,
		0,
		1,
		true,
		"a string",
		map[int64]string{1: "one", 11: "oneone", 21: "twoone", 1211: "onetwooneone"},
		struct {
			A int
			B *string
			C bool
		}{4, &v, false},
	}

	for _, test := range tests {
		var results []string
		for i := 0; i < 10; i++ {
			data, err := jsonEnc(test)
			if err != nil {
				t.Fatalf("Failed to encode %v: %v", tests, err)
			}
			results = append(results, string(data))
		}
		for i, data := range results {
			if data != results[0] {
				t.Errorf("coder not deterministic: data[%d]: %v != %v ", i, data, results[0])
			}
		}

		decoded, err := jsonDec(reflect.TypeOf(test), []byte(results[0]))
		if err != nil {
			t.Fatalf("Failed to decode: %v", err)
		}

		if !reflect.DeepEqual(decoded, test) {
			t.Errorf("Corrupt coding: %v, want %v", decoded, test)
		}
	}
}

func TestCoders(t *testing.T) {
	ptrString := "test *string"
	tests := []interface{}{
		43,
		12431235,
		-2,
		0,
		1,
		true,
		"a string",
		map[int64]string{1: "one", 11: "oneone", 21: "twoone", 1211: "onetwooneone"},
		struct {
			A int
			B *string
			C bool
		}{4, &ptrString, false},
		[...]int64{1, 2, 3, 4, 5}, // array
		[]int64{1, 2, 3, 4, 5},    // slice
		struct {
			A []int
			B [3]int
		}{A: []int{1, 2, 3}, B: [...]int{4, 5, 6}},
	}

	for _, test := range tests {
		var results []string
		rt := reflect.TypeOf(test)
		enc := NewElementEncoder(rt)
		for i := 0; i < 10; i++ {
			var buf bytes.Buffer
			if err := enc.Encode(test, &buf); err != nil {
				t.Fatalf("Failed to encode %v: %v", tests, err)
			}
			results = append(results, string(buf.Bytes()))
		}
		for i, d := range results {
			if d != results[0] {
				t.Errorf("coder not deterministic: encoding %d not the same as the first encoding: %v != %v ", i, d, results[0])
			}
		}

		dec := NewElementDecoder(rt)
		buf := bytes.NewBuffer([]byte(results[0]))
		decoded, err := dec.Decode(buf)
		if err != nil {
			t.Fatalf("Failed to decode: %v", err)
		}

		if !reflect.DeepEqual(decoded, test) {
			t.Errorf("Corrupt coding: %v, want %v", decoded, test)
		}
	}
}
