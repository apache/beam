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
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

type symlookup bool

func (s symlookup) Sym2Addr(name string) (uintptr, error) {
	switch name {
	case reflectx.FunctionName(dec):
		return reflect.ValueOf(dec).Pointer(), nil
	case reflectx.FunctionName(enc):
		return reflect.ValueOf(enc).Pointer(), nil
	default:
		panic(fmt.Sprintf("bad name: %v", name))
	}
}

func init() {
	runtime.SymbolResolver = symlookup(false)
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
			"W<KV<foo,bar>>",
			coder.NewWKV([]*coder.Coder{foo, bar}, window.NewGlobalWindow()),
		},
		{
			"W<GBK<foo,bar>>",
			coder.NewWGBK([]*coder.Coder{foo, bar}, window.NewGlobalWindow()),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coders, err := UnmarshalCoders(MarshalCoders([]*coder.Coder{test.c}))
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
