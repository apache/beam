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
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
)

func makeValidGBKMultiEdge(keyCoder, valueCoder *coder.Coder) *graph.MultiEdge {
	inNode := &graph.Node{Coder: coder.NewKV([]*coder.Coder{keyCoder, valueCoder})}
	in := &graph.Inbound{From: inNode}
	inputs := []*graph.Inbound{in}
	return &graph.MultiEdge{Op: graph.CoGBK, Input: inputs}
}

var tests = []struct {
	name       string
	keyCoder   *coder.Coder
	valueCoder *coder.Coder
}{
	{
		"bytes",
		coder.NewBytes(),
		coder.NewBytes(),
	},
	{
		"bools",
		coder.NewBool(),
		coder.NewBool(),
	},
	{
		"varint",
		coder.NewVarInt(),
		coder.NewVarInt(),
	},
	{
		"strings",
		coder.NewString(),
		coder.NewString(),
	},
}

func TestMakeKVUnionCoder(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := makeValidGBKMultiEdge(test.keyCoder, test.valueCoder)
			c, err := MakeKVUnionCoder(input)
			if err != nil {
				t.Fatalf("MakeKVUnionCoder(%v) failed, got %v", input, err)
			}
			if got, want := c.Kind, coder.KV; got != want {
				t.Errorf("got coder kind %v, want %v", got, want)
			}
			if got, want := c.Components[0].Kind, test.keyCoder.Kind; got != want {
				t.Errorf("got K coder kind %v, want %v", got, want)
			}
		})
	}
}

func TestMakeKVUnionCoder_bad(t *testing.T) {
	input := &graph.MultiEdge{Op: graph.Impulse}
	c, err := MakeKVUnionCoder(input)
	if err == nil {
		t.Errorf("makeKVUnionCoder(%v) succeeded when it should have failed, got coder %v", input, c)
	}
}

func TestMakeGBKUnionCoder(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := makeValidGBKMultiEdge(test.keyCoder, test.valueCoder)
			c, err := MakeGBKUnionCoder(input)
			if err != nil {
				t.Fatalf("MakeGBKUnionCoder(%v) failed, got %v", input, err)
			}
			if got, want := c.Kind, coder.CoGBK; got != want {
				t.Errorf("got coder kind %v, want %v", got, want)
			}
			if got, want := c.Components[0].Kind, test.keyCoder.Kind; got != want {
				t.Errorf("got K coder kind %v, want %v", got, want)
			}
		})
	}
}

func TestMakeGBKUnionCoder_bad(t *testing.T) {
	input := &graph.MultiEdge{Op: graph.Impulse}
	c, err := MakeGBKUnionCoder(input)
	if err == nil {
		t.Errorf("makeGBKUnionCoder(%v) succeeded when it should have failed, got coder %v", input, c)
	}
}

func TestMakeUnionCoder(t *testing.T) {
	c, err := makeUnionCoder()
	if err != nil {
		t.Fatalf("makeUnionCoder() failed, got %v", err)
	}
	if c.Kind != coder.KV {
		t.Fatalf("got coder kind %v, want KV", c.Kind)
	}
	if got, want := c.Components[0].Kind, coder.Custom; got != want {
		t.Errorf("got K coder kind %v, want %v", got, want)
	}
	if got, want := c.Components[1].Kind, coder.Bytes; got != want {
		t.Errorf("got V coder kind %v, want %v", got, want)
	}
}
