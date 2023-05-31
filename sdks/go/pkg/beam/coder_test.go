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
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
)

func TestJSONCoder(t *testing.T) {
	v := "teststring"
	tests := []any{
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
				t.Fatalf("Failed to encode %v: %v", test, err)
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

func TestSchemaCoder(t *testing.T) {
	v := "teststring"
	tests := []any{
		struct {
			A int
			B *string
			C bool
		}{4, &v, false},
		&struct {
			A int
			B *string
			C bool
		}{4, &v, false},
		struct {
			A map[string]int
			B []int
		}{map[string]int{"three": 3}, []int{4}},
		struct {
			A map[string]int
			B [1]int
		}{map[string]int{"three": 3}, [...]int{4}},
	}

	for _, test := range tests {
		rt := reflect.TypeOf(test)
		t.Run(fmt.Sprintf("%v", rt), func(t *testing.T) {
			var results []string
			for i := 0; i < 10; i++ {
				data, err := schemaEnc(rt, test)
				if err != nil {
					t.Fatalf("Failed to encode %v: %v", test, err)
				}
				results = append(results, string(data))
			}
			for i, data := range results {
				if data != results[0] {
					t.Errorf("coder not deterministic: data[%d]: %v != %v ", i, data, results[0])
				}
			}

			decoded, err := schemaDec(rt, []byte(results[0]))
			if err != nil {
				t.Fatalf("Failed to decode: %v", err)
			}

			if !reflect.DeepEqual(decoded, test) {
				t.Errorf("Corrupt coding: %v, want %v", decoded, test)
			}
		})
	}
}

func TestCoders(t *testing.T) {
	ptrString := "test *string"
	type regTestType struct {
		A [4]int
	}
	schema.RegisterType(reflect.TypeOf((*regTestType)(nil)))
	tests := []any{
		43,
		12431235,
		-2,
		0,
		1,
		true,
		"a string",
		map[int64]string{1: "one", 11: "oneone", 21: "twoone", 1211: "onetwooneone"}, // (not supported by custom type registration)
		struct {
			A int
			B *string
			C bool
		}{4, &ptrString, false},
		[...]int64{1, 2, 3, 4, 5}, // array (not supported by custom type registration)
		[]int64{1, 2, 3, 4, 5},    // slice
		struct {
			A []int
			B [3]int
		}{A: []int{1, 2, 3}, B: [...]int{4, 5, 6}},
		[...]struct{ A int }{{1}, {2}, {3}, {4}, {5}},
		[]struct{ B int }{{1}, {2}, {3}, {4}, {5}},
		regTestType{[4]int{4, 2, 4, 2}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%T", test), func(t *testing.T) {
			var results []string
			rt := reflect.TypeOf(test)
			enc := NewElementEncoder(rt)
			for i := 0; i < 10; i++ {
				var buf bytes.Buffer
				if err := enc.Encode(test, &buf); err != nil {
					t.Fatalf("Failed to encode %v: %v", test, err)
				}
				results = append(results, buf.String())
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
				t.Fatalf("Failed to decode: %q, %v", results[0], err)
			}

			if !reflect.DeepEqual(decoded, test) {
				t.Errorf("Corrupt coding: %v, want %v", decoded, test)
			}
		})
	}
}
