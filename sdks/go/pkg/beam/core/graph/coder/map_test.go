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

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/google/go-cmp/cmp"
)

func TestEncodeDecodeMap(t *testing.T) {
	byteEnc := containerEncoderForType(reflectx.Uint8)
	byteDec := containerDecoderForType(reflectx.Uint8)
	bytePtrEnc := containerEncoderForType(reflect.PtrTo(reflectx.Uint8))
	bytePtrDec := containerDecoderForType(reflect.PtrTo(reflectx.Uint8))

	ptrByte := byte(42)

	tests := []struct {
		v          interface{}
		encK, encV func(reflect.Value, io.Writer) error
		decK, decV func(reflect.Value, io.Reader) error
		encoded    []byte
		decodeOnly bool
	}{
		{
			v:       map[byte]byte{10: 42},
			encK:    byteEnc,
			encV:    byteEnc,
			decK:    byteDec,
			decV:    byteDec,
			encoded: []byte{0, 0, 0, 1, 10, 42},
		}, {
			v:       map[byte]*byte{10: &ptrByte},
			encK:    byteEnc,
			encV:    bytePtrEnc,
			decK:    byteDec,
			decV:    bytePtrDec,
			encoded: []byte{0, 0, 0, 1, 10, 1, 42},
		}, {
			v:          map[byte]*byte{10: &ptrByte, 23: nil, 53: nil},
			encK:       byteEnc,
			encV:       bytePtrEnc,
			decK:       byteDec,
			decV:       bytePtrDec,
			encoded:    []byte{0, 0, 0, 3, 10, 1, 42, 23, 0, 53, 0},
			decodeOnly: true,
		},
	}
	for _, test := range tests {
		test := test
		if !test.decodeOnly {
			t.Run(fmt.Sprintf("encode %q", test.v), func(t *testing.T) {
				var buf bytes.Buffer
				err := mapEncoder(reflect.TypeOf(test.v), test.encK, test.encV)(reflect.ValueOf(test.v), &buf)
				if err != nil {
					t.Fatalf("mapEncoder(%q) = %v", test.v, err)
				}
				if d := cmp.Diff(test.encoded, buf.Bytes()); d != "" {
					t.Errorf("mapEncoder(%q) = %v, want %v diff(-want,+got):\n %v", test.v, buf.Bytes(), test.encoded, d)
				}
			})
		}
		t.Run(fmt.Sprintf("decode %v", test.v), func(t *testing.T) {
			buf := bytes.NewBuffer(test.encoded)
			rt := reflect.TypeOf(test.v)
			var dec func(reflect.Value, io.Reader) error
			dec = mapDecoder(rt, test.decK, test.decV)
			rv := reflect.New(rt).Elem()
			err := dec(rv, buf)
			if err != nil {
				t.Fatalf("mapDecoder(%q) = %v", test.encoded, err)
			}
			got := rv.Interface()
			if d := cmp.Diff(test.v, got); d != "" {
				t.Errorf("mapDecoder(%q) = %q, want %v diff(-want,+got):\n %v", test.encoded, got, test.v, d)
			}
		})
	}
}
