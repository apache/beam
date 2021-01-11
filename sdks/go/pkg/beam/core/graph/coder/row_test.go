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
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestReflectionRowCoderGeneration(t *testing.T) {
	num := 35
	tests := []struct {
		want interface{}
	}{{
		// Top level value check
		want: UserType1{
			A: "cats",
			B: 24,
			C: "pjamas",
		},
	}, {
		// Top level pointer check
		want: &UserType1{
			A: "marmalade",
			B: 24,
			C: "jam",
		},
	}, {
		// Inner pointer check.
		want: UserType2{
			A: "dogs",
			B: &UserType1{
				A: "cats",
				B: 24,
				C: "pjamas",
			},
			C: &num,
		},
	}, {
		// nil pointer check.
		want: UserType2{
			A: "dogs",
			B: nil,
			C: nil,
		},
	}, {
		// All zeroes
		want: struct {
			V00 bool
			V01 byte  // unsupported by spec (same as uint8)
			V02 uint8 // unsupported by spec
			V03 int16
			//	V04 uint16 // unsupported by spec
			V05 int32
			//	V06 uint32 // unsupported by spec
			V07 int64
			//	V08 uint64 // unsupported by spec
			V09 int
			V10 struct{}
			V11 *struct{}
			V12 [0]int
			V13 [2]int
			V14 []int
			V15 map[string]int
			V16 float32
			V17 float64
			V18 []byte
			V19 [2]*int
			V20 map[*string]*int
		}{},
	}, {
		want: struct {
			V00 bool
			V01 byte  // unsupported by spec (same as uint8)
			V02 uint8 // unsupported by spec
			V03 int16
			//	V04 uint16 // unsupported by spec
			V05 int32
			//	V06 uint32 // unsupported by spec
			V07 int64
			//	V08 uint64 // unsupported by spec
			V09 int
			V10 struct{}
			V11 *struct{}
			V12 [0]int
			V13 [2]int
			V14 []int
			V15 map[string]int
			V16 float32
			V17 float64
			V18 []byte
			V19 [2]*int
			V20 map[string]*int
			V21 []*int
		}{
			V00: true,
			V01: 1,
			V02: 2,
			V03: 3,
			V05: 5,
			V07: 7,
			V09: 9,
			V10: struct{}{},
			V11: &struct{}{},
			V12: [0]int{},
			V13: [2]int{72, 908},
			V14: []int{12, 9326, 641346, 6},
			V15: map[string]int{"pants": 42},
			V16: 3.14169,
			V17: 2.6e100,
			V18: []byte{21, 17, 65, 255, 0, 16},
			V19: [2]*int{nil, &num},
			V20: map[string]*int{
				"notnil": &num,
				"nil":    nil,
			},
			V21: []*int{nil, &num, nil},
		},
		// TODO add custom types such as protocol buffers.
	},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%+v", test.want), func(t *testing.T) {
			rt := reflect.TypeOf(test.want)
			enc, err := RowEncoderForStruct(rt)
			if err != nil {
				t.Fatalf("RowEncoderForStruct(%v) = %v, want nil error", rt, err)
			}
			var buf bytes.Buffer
			if err := enc(test.want, &buf); err != nil {
				t.Fatalf("enc(%v) = err, want nil error", err)
			}
			dec, err := RowDecoderForStruct(rt)
			if err != nil {
				t.Fatalf("RowDecoderForStruct(%v) = %v, want nil error", rt, err)
			}
			b := buf.Bytes()
			r := bytes.NewBuffer(b)
			got, err := dec(r)
			if err != nil {
				t.Fatalf("RowDecoderForStruct(%v) = %v, want nil error", rt, err)
			}
			if d := cmp.Diff(test.want, got); d != "" {
				t.Fatalf("dec(enc(%v)) = %v\ndiff (-want, +got): %v", test.want, got, d)
			}
		})
	}

}

type UserType1 struct {
	A string
	B int
	C string
}

type UserType2 struct {
	A string
	B *UserType1
	C *int
}
