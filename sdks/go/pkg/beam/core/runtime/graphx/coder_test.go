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
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

func init() {
	runtime.RegisterFunction(dec)
	runtime.RegisterFunction(enc)
}

// TestMarshalUnmarshalCoders verifies that coders survive a proto roundtrip.
func TestMarshalUnmarshalCoders(t *testing.T) {
	foo := custom("foo", reflectx.Bool)
	bar := custom("bar", reflectx.String)
	baz := custom("baz", reflectx.Int)

	tests := []struct {
		name string
		c    *coder.Coder
	}{
		{
			"bytes",
			coder.NewBytes(),
		},
		{
			"varint",
			coder.NewVarInt(),
		},
		{
			"foo",
			foo,
		},
		{
			"bar",
			bar,
		},
		{
			"baz",
			baz,
		},
		{
			"W<bytes>",
			coder.NewW(coder.NewBytes(), window.NewGlobalWindow()),
		},
		{
			"KV<foo,bar>",
			coder.NewKV([]*coder.Coder{foo, bar}),
		},
		{
			"CoGBK<foo,bar>",
			coder.NewCoGBK([]*coder.Coder{foo, bar}),
		},
		{
			"CoGBK<foo,bar,baz>",
			coder.NewCoGBK([]*coder.Coder{foo, bar, baz}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coders, err := graphx.UnmarshalCoders(graphx.MarshalCoders([]*coder.Coder{test.c}))
			if err != nil {
				t.Fatalf("Unmarshal(Marshal(%v)) failed: %v", test.c, err)
			}
			if len(coders) != 1 || !test.c.Equals(coders[0]) {
				t.Errorf("Unmarshal(Marshal(%v)) = %v, want identity", test.c, coders)
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
