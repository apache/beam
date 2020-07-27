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
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/google/go-cmp/cmp"
)

func TestEncodeBool(t *testing.T) {
	tests := []struct {
		v    bool
		want []byte
	}{
		{v: false, want: []byte{0}},
		{v: true, want: []byte{1}},
	}
	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("%v", test.v), func(t *testing.T) {
			var buf bytes.Buffer
			err := EncodeBool(test.v, &buf)
			if err != nil {
				t.Fatalf("EncodeBool(%v) = %v", test.v, err)
			}
			if d := cmp.Diff(test.want, buf.Bytes()); d != "" {
				t.Errorf("EncodeBool(%v) = %v, want %v diff(-want,+got):\n %v", test.v, buf.Bytes(), test.want, d)
			}
		})
	}
}

func TestDecodeBool(t *testing.T) {
	tests := []struct {
		b    []byte
		want bool
		err  error
	}{
		{want: false, b: []byte{0}},
		{want: true, b: []byte{1}},
		{b: []byte{42}, err: errors.Errorf("error decoding bool: received invalid value %v", 42)},
	}
	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("%v", test.want), func(t *testing.T) {
			buf := bytes.NewBuffer(test.b)
			got, err := DecodeBool(buf)
			if test.err != nil && err != nil {
				if d := cmp.Diff(test.err.Error(), err.Error()); d != "" {
					t.Errorf("DecodeBool(%v) = %v, want %v diff(-want,+got):\n %v", test.b, err, test.err, d)
				}
				return
			}
			if err != nil {
				t.Fatalf("DecodeBool(%v) = %v", test.b, err)
			}
			if d := cmp.Diff(test.want, got); d != "" {
				t.Errorf("DecodeBool(%v) = %v, want %v diff(-want,+got):\n %v", test.b, got, test.want, d)
			}
		})
	}
}
