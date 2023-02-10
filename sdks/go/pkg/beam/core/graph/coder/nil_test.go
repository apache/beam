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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/google/go-cmp/cmp"
)

func TestEncodeDecodeNullable(t *testing.T) {
	byteEnc := func(v reflect.Value, w io.Writer) error {
		return EncodeByte(byte(v.Uint()), w)
	}
	byteDec := func(v reflect.Value, r io.Reader) error {
		b, err := DecodeByte(r)
		if err != nil {
			return errors.Wrap(err, "error decoding single byte field")
		}
		v.SetUint(uint64(b))
		return nil
	}
	bytePtrEnc := func(v reflect.Value, w io.Writer) error {
		return byteEnc(v.Elem(), w)
	}
	bytePtrDec := func(v reflect.Value, r io.Reader) error {
		v.Set(reflect.New(reflectx.Uint8))
		return byteDec(v.Elem(), r)
	}
	byteCtnrPtrEnc := NullableEncoder(bytePtrEnc)
	byteCtnrPtrDec := NullableDecoder(bytePtrDec)

	tests := []struct {
		decoded any
		encoded []byte
	}{
		{
			decoded: (*byte)(nil),
			encoded: []byte{0},
		},
		{
			decoded: create(10),
			encoded: []byte{1, 10},
		},
		{
			decoded: create(20),
			encoded: []byte{1, 20},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("encode %q", test.encoded), func(t *testing.T) {
			var buf bytes.Buffer
			encErr := byteCtnrPtrEnc(reflect.ValueOf(test.decoded), &buf)
			if encErr != nil {
				t.Fatalf("NullableEncoder(%q) = %v", test.decoded, encErr)
			}
			if d := cmp.Diff(test.encoded, buf.Bytes()); d != "" {
				t.Errorf("NullableEncoder(%q) = %v, want %v diff(-want,+got):\n %v", test.decoded, buf.Bytes(), test.encoded, d)
			}
		})
		t.Run(fmt.Sprintf("decode %q", test.decoded), func(t *testing.T) {
			buf := bytes.NewBuffer(test.encoded)
			rv := reflect.New(reflect.TypeOf(test.decoded)).Elem()
			decErr := byteCtnrPtrDec(rv, buf)
			if decErr != nil {
				t.Fatalf("NullableDecoder(%q) = %v", test.encoded, decErr)
			}
			if d := cmp.Diff(test.decoded, rv.Interface()); d != "" {
				t.Errorf("NullableDecoder (%q) = %q, want %v diff(-want,+got):\n %v", test.encoded, rv.Interface(), test.decoded, d)
			}
		})
	}

}

func create(x byte) *byte {
	return &x
}
