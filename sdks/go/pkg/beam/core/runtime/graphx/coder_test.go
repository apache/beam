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

package graphx_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

func init() {
	runtime.RegisterFunction(dec)
	runtime.RegisterFunction(enc)
}

type registeredNamedTypeForTest struct {
	A, B int64
	C    string
}

func init() {
	schema.RegisterType(reflect.TypeOf((*registeredNamedTypeForTest)(nil)))
}

// TestMarshalUnmarshalCoders verifies that coders survive a proto roundtrip.
func TestMarshalUnmarshalCoders(t *testing.T) {
	foo := custom("foo", reflectx.Bool)
	bar := custom("bar", reflectx.String)
	baz := custom("baz", reflectx.Int)

	tests := []struct {
		name       string
		c          *coder.Coder
		equivalent *coder.Coder
	}{
		{
			name: "bytes",
			c:    coder.NewBytes(),
		},
		{
			name: "bool",
			c:    coder.NewBool(),
		},
		{
			name: "varint",
			c:    coder.NewVarInt(),
		},
		{
			name: "double",
			c:    coder.NewDouble(),
		},
		{
			name: "string",
			c:    coder.NewString(),
		},
		{
			name: "foo",
			c:    foo,
		},
		{
			name: "bar",
			c:    bar,
		},
		{
			name: "baz",
			c:    baz,
		},
		{
			name: "IW",
			c:    coder.NewIntervalWindowCoder(),
		},
		{
			name: "W<bytes>",
			c:    coder.NewW(coder.NewBytes(), coder.NewGlobalWindow()),
		},
		{
			name: "N<bytes>",
			c:    coder.NewN(coder.NewBytes()),
		},
		{
			name: "I<foo>",
			c:    coder.NewI(foo),
		},
		{
			name: "KV<foo,bar>",
			c:    coder.NewKV([]*coder.Coder{foo, bar}),
		},
		{
			name: "KV<foo, I<bar>>",
			c:    coder.NewKV([]*coder.Coder{foo, coder.NewI(bar)}),
		},
		{
			name:       "CoGBK<foo,bar>",
			c:          coder.NewCoGBK([]*coder.Coder{foo, bar}),
			equivalent: coder.NewKV([]*coder.Coder{foo, coder.NewI(bar)}),
		},
		{
			name: "CoGBK<foo,bar,baz>",
			c:    coder.NewCoGBK([]*coder.Coder{foo, bar, baz}),
		},
		{
			name: "R[graphx.registeredNamedTypeForTest]",
			c:    coder.NewR(typex.New(reflect.TypeOf((*registeredNamedTypeForTest)(nil)).Elem())),
		},
		{
			name: "R[*graphx.registeredNamedTypeForTest]",
			c:    coder.NewR(typex.New(reflect.TypeOf((*registeredNamedTypeForTest)(nil)))),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ids, marshalCoders, err := graphx.MarshalCoders([]*coder.Coder{test.c})
			if err != nil {
				t.Fatalf("Marshal(%v) failed: %v", test.c, err)
			}
			coders, err := graphx.UnmarshalCoders(ids, marshalCoders)
			if err != nil {
				t.Fatalf("Unmarshal(Marshal(%v)) failed: %v", test.c, err)
			}
			if test.equivalent != nil && !test.equivalent.Equals(coders[0]) {
				t.Errorf("Unmarshal(Marshal(%v)) = %v, want equivalent", test.equivalent, coders)
			}
			if test.equivalent == nil && !test.c.Equals(coders[0]) {
				t.Errorf("Unmarshal(Marshal(%v)) = %v, want identity", test.c, coders)
			}
		})
	}

	for _, test := range tests {
		t.Run("namespaced:"+test.name, func(t *testing.T) {
			cm := graphx.NewCoderMarshaller()
			cm.Namespace = "testnamespace"
			ids, err := cm.AddMulti([]*coder.Coder{test.c})
			if err != nil {
				t.Fatalf("AddMulti(%v) failed: %v", test.c, err)
			}
			marshalCoders := cm.Build()
			for _, id := range ids {
				if !strings.Contains(id, cm.Namespace) {
					t.Errorf("got %v, want it to contain %v", id, cm.Namespace)
				}
			}

			coders, err := graphx.UnmarshalCoders(ids, marshalCoders)
			if err != nil {
				t.Fatalf("Unmarshal(Marshal(%v)) failed: %v", test.c, err)
			}
			if test.equivalent != nil && !test.equivalent.Equals(coders[0]) {
				t.Errorf("Unmarshal(Marshal(%v)) = %v, want equivalent", test.equivalent, coders)
			}
			if test.equivalent == nil && !test.c.Equals(coders[0]) {
				t.Errorf("Unmarshal(Marshal(%v)) = %v, want identity", test.c, coders)
			}
		})
	}

	// These tests cover the pure dataflow to dataflow coder cases.
	for _, test := range tests {
		t.Run("dataflow:"+test.name, func(t *testing.T) {
			ref, err := graphx.EncodeCoderRef(test.c)
			if err != nil {
				t.Fatalf("EncodeCoderRef(%v) failed: %v", test.c, err)
			}
			got, err := graphx.DecodeCoderRef(ref)
			if err != nil {
				t.Fatalf("DecodeCoderRef(EncodeCoderRef(%v)) failed: %v", test.c, err)
			}
			if test.equivalent != nil && !test.equivalent.Equals(got) {
				t.Errorf("DecodeCoderRef(EncodeCoderRef(%v)) = %v want equivalent", test.equivalent, got)
			}
			if test.equivalent == nil && !test.c.Equals(got) {
				t.Errorf("DecodeCoderRef(EncodeCoderRef(%v)) = %v want identity", test.c, got)
			}
		})
	}
}

func enc(in typex.T) ([]byte, error) {
	panic("enc is fake")
}

func dec(t reflect.Type, in []byte) (typex.T, error) {
	panic("dec is fake")
}

func custom(name string, t reflect.Type) *coder.Coder {
	c, _ := coder.NewCustomCoder(name, t, enc, dec)
	return &coder.Coder{Kind: coder.Custom, T: typex.New(t), Custom: c}
}
