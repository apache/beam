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

package graphx

import (
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

func init() {
	runtime.RegisterFunction(emptyFn)
	runtime.RegisterFunction(oneArg)
	runtime.RegisterFunction(oneRet)
	runtime.RegisterFunction(argAndRet)
}

func TestEncodeDecodeType(t *testing.T) {
	var tests = []struct {
		name   string
		inType reflect.Type
	}{
		{
			"bool",
			reflect.TypeOf(true),
		},
		{
			"string",
			reflect.TypeOf("string"),
		},
		{
			"float",
			reflect.TypeOf(3.14159),
		},
		{
			"int64",
			reflect.TypeOf(int64(64)),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			enc, err := EncodeType(test.inType)
			if err != nil {
				t.Fatalf("failed to encode type %v, got err %v", test.inType, err)
			}
			decType, err := DecodeType(enc)
			if err != nil {
				t.Fatalf("failed to decode input %v, got err %v", enc, err)
			}
			if got, want := decType, test.inType; got != want {
				t.Errorf("type mismatch, got %v, want %v", got, want)
			}
		})
	}
}

func funcEquals(got, want reflectx.Func) bool {
	return got.Name() == want.Name() && got.Type() == want.Type()
}

func emptyFn() {}

func oneArg(x int) {}

func oneRet() error { return nil }

func argAndRet(x string) string { return x }

func TestEncodeDecodeFn(t *testing.T) {
	var tests = []struct {
		name string
		inFn any
	}{
		{
			"no arg or return",
			emptyFn,
		},
		{
			"one arg, no return",
			oneArg,
		},
		{
			"no arg, one return",
			oneRet,
		},
		{
			"one arg, one return",
			argAndRet,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rFn := reflectx.MakeFunc(test.inFn)
			enc, err := EncodeFn(rFn)
			if err != nil {
				t.Fatalf("failed to encode func %v, got err %v", rFn, err)
			}
			decFn, err := DecodeFn(enc)
			if err != nil {
				t.Fatalf("failed to decode input %v, got err %v", enc, err)
			}
			if got, want := decFn, rFn; !funcEquals(got, want) {
				t.Errorf("func mismatch, got %v, want %v", got, want)
			}
		})
	}
}

func TestEncodeDecodeCoder(t *testing.T) {
	var tests = []struct {
		name    string
		inCoder *coder.Coder
	}{
		{
			"bytes",
			coder.NewBytes(),
		},
		{
			"bool",
			coder.NewBool(),
		},
		{
			"double",
			coder.NewDouble(),
		},
		{
			"string",
			coder.NewString(),
		},
		{
			"var int",
			coder.NewVarInt(),
		},
		{
			"windowed",
			coder.NewW(coder.NewBytes(), coder.NewGlobalWindow()),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			enc, err := EncodeCoder(test.inCoder)
			if err != nil {
				t.Fatalf("failed to encode coder %v, got err %v", test.inCoder, err)
			}
			decCoder, err := DecodeCoder(enc)
			if err != nil {
				t.Fatalf("failed to decode input %v, got err %v", enc, err)
			}
			if got, want := decCoder, test.inCoder; !got.Equals(want) {
				t.Errorf("coder mismatch, got %v, want %v", got, want)
			}
		})
	}
}
