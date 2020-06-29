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
	"io"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestEncodeDecodeIterable(t *testing.T) {
	stringEnc := func(rv reflect.Value, w io.Writer) error {
		return EncodeStringUTF8(rv.String(), w)
	}
	stringDec := func(rv reflect.Value, r io.Reader) error {
		v, err := DecodeStringUTF8(r)
		if err != nil {
			return nil
		}
		t.Log(v)
		rv.SetString(v)
		return nil
	}

	tests := []struct {
		v          interface{}
		encElm     func(reflect.Value, io.Writer) error
		decElm     func(reflect.Value, io.Reader) error
		encoded    []byte
		decodeOnly bool
	}{
		{
			v: [4]byte{1, 2, 3, 4},
			encElm: func(rv reflect.Value, w io.Writer) error {
				return EncodeByte(byte(rv.Uint()), w)
			},
			decElm: func(rv reflect.Value, r io.Reader) error {
				b, err := DecodeByte(r)
				if err != nil {
					return nil
				}
				rv.SetUint(uint64(b))
				return nil
			},
			encoded: []byte{0, 0, 0, 4, 1, 2, 3, 4},
		},
		{
			v:       []string{"my", "gopher"},
			encElm:  stringEnc,
			decElm:  stringDec,
			encoded: []byte{0, 0, 0, 2, 2, 'm', 'y', 6, 'g', 'o', 'p', 'h', 'e', 'r'},
		},
		{
			v:          []string{"my", "gopher", "rocks"},
			encElm:     stringEnc,
			decElm:     stringDec,
			encoded:    []byte{255, 255, 255, 255, 1, 2, 'm', 'y', 2, 6, 'g', 'o', 'p', 'h', 'e', 'r', 5, 'r', 'o', 'c', 'k', 's', 0},
			decodeOnly: true,
		},
	}
	for _, test := range tests {
		test := test
		if !test.decodeOnly {
			t.Run(fmt.Sprintf("encode %q", test.v), func(t *testing.T) {
				var buf bytes.Buffer
				err := iterableEncoder(reflect.TypeOf(test.v), test.encElm)(reflect.ValueOf(test.v), &buf)
				if err != nil {
					t.Fatalf("EncodeBytes(%q) = %v", test.v, err)
				}
				if d := cmp.Diff(test.encoded, buf.Bytes()); d != "" {
					t.Errorf("EncodeBytes(%q) = %v, want %v diff(-want,+got):\n %v", test.v, buf.Bytes(), test.encoded, d)
				}
			})
		}
		t.Run(fmt.Sprintf("decode %v", test.v), func(t *testing.T) {
			buf := bytes.NewBuffer(test.encoded)
			rt := reflect.TypeOf(test.v)
			var dec func(reflect.Value, io.Reader) error
			switch rt.Kind() {
			case reflect.Slice:
				dec = iterableDecoderForSlice(rt, test.decElm)
			case reflect.Array:
				dec = iterableDecoderForArray(rt, test.decElm)
			}
			rv := reflect.New(rt).Elem()
			err := dec(rv, buf)
			if err != nil {
				t.Fatalf("DecodeBytes(%q) = %v", test.encoded, err)
			}
			got := rv.Interface()
			if d := cmp.Diff(test.v, got); d != "" {
				t.Errorf("DecodeBytes(%q) = %q, want %v diff(-want,+got):\n %v", test.encoded, got, test.v, d)
			}
		})
	}
}
