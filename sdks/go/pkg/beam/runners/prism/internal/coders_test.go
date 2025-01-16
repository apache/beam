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

package internal

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/engine"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func Test_isLeafCoder(t *testing.T) {
	tests := []struct {
		urn    string
		isLeaf bool
	}{
		{urns.CoderBytes, true},
		{urns.CoderStringUTF8, true},
		{urns.CoderLengthPrefix, true},
		{urns.CoderVarInt, true},
		{urns.CoderDouble, true},
		{urns.CoderBool, true},
		{urns.CoderGlobalWindow, true},
		{urns.CoderIntervalWindow, true},
		{urns.CoderIterable, false},
		{urns.CoderRow, false},
		{urns.CoderKV, false},
	}
	for _, test := range tests {
		undertest := pipepb.Coder_builder{
			Spec: pipepb.FunctionSpec_builder{
				Urn: test.urn,
			}.Build(),
		}.Build()
		if got, want := isLeafCoder(undertest), test.isLeaf; got != want {
			t.Errorf("isLeafCoder(%v) = %v, want %v", test.urn, got, want)
		}
	}
}

func Test_makeWindowedValueCoder(t *testing.T) {
	coders := map[string]*pipepb.Coder{}

	gotID, err := makeWindowedValueCoder("testPID", pipepb.Components_builder{
		Pcollections: map[string]*pipepb.PCollection{
			"testPID": pipepb.PCollection_builder{CoderId: "testCoderID"}.Build(),
		},
		Coders: map[string]*pipepb.Coder{
			"testCoderID": pipepb.Coder_builder{
				Spec: pipepb.FunctionSpec_builder{
					Urn: urns.CoderBool,
				}.Build(),
			}.Build(),
		},
	}.Build(), coders)
	if err != nil {
		t.Errorf("makeWindowedValueCoder(...) = error %v, want nil", err)
	}
	if gotID == "" {
		t.Errorf("makeWindowedValueCoder(...) = %v, want non-empty", gotID)
	}
	got := coders[gotID]
	if got == nil {
		t.Errorf("makeWindowedValueCoder(...) = ID %v, had nil entry", gotID)
	}
	if got.GetSpec().GetUrn() != urns.CoderWindowedValue {
		t.Errorf("makeWindowedValueCoder(...) = ID %v, had nil entry", gotID)
	}
}

func Test_makeWindowCoders(t *testing.T) {
	tests := []struct {
		urn       string
		window    typex.Window
		coderType engine.WinCoderType
	}{
		{urns.CoderGlobalWindow, window.GlobalWindow{}, engine.WinGlobal},
		{urns.CoderIntervalWindow, window.IntervalWindow{
			Start: mtime.MinTimestamp,
			End:   mtime.MaxTimestamp,
		}, engine.WinInterval},
	}
	for _, test := range tests {
		undertest := pipepb.Coder_builder{
			Spec: pipepb.FunctionSpec_builder{
				Urn: test.urn,
			}.Build(),
		}.Build()
		gotCoderType, dec, enc := makeWindowCoders(undertest)

		if got, want := gotCoderType, test.coderType; got != want {
			t.Errorf("makeWindowCoders returned different coder type: got %v, want %v", got, want)
		}

		// Validate we're getting a round trip coder.
		var buf bytes.Buffer
		if err := enc.EncodeSingle(test.window, &buf); err != nil {
			t.Errorf("encoder[%v].EncodeSingle(%v) = %v, want nil", test.urn, test.window, err)
		}
		got, err := dec.DecodeSingle(&buf)
		if err != nil {
			t.Errorf("decoder[%v].DecodeSingle(%v) = %v, want nil", test.urn, test.window, err)
		}

		if want := test.window; got != want {
			t.Errorf("makeWindowCoders(%v) didn't round trip: got %v, want %v", test.urn, got, want)
		}
	}
}

func Test_lpUnknownCoders(t *testing.T) {
	tests := []struct {
		name         string
		urn          string
		components   []string
		bundle, base map[string]*pipepb.Coder
		want         map[string]*pipepb.Coder
	}{
		{"alreadyProcessed",
			urns.CoderBool, nil,
			map[string]*pipepb.Coder{
				"test": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBool}.Build()}.Build(),
			},
			map[string]*pipepb.Coder{},
			map[string]*pipepb.Coder{
				"test": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBool}.Build()}.Build(),
			},
		},
		{"alreadyProcessedLP",
			urns.CoderBool, nil,
			map[string]*pipepb.Coder{
				"test_lp": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderLengthPrefix}.Build(), ComponentCoderIds: []string{"test"}}.Build(),
				"test":    pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBool}.Build()}.Build(),
			},
			map[string]*pipepb.Coder{},
			map[string]*pipepb.Coder{
				"test_lp": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderLengthPrefix}.Build(), ComponentCoderIds: []string{"test"}}.Build(),
				"test":    pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBool}.Build()}.Build(),
			},
		},
		{"noNeedForLP",
			urns.CoderBool, nil,
			map[string]*pipepb.Coder{},
			map[string]*pipepb.Coder{},
			map[string]*pipepb.Coder{
				"test": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBool}.Build()}.Build(),
			},
		},
		{"needLP",
			urns.CoderRow, nil,
			map[string]*pipepb.Coder{},
			map[string]*pipepb.Coder{},
			map[string]*pipepb.Coder{
				"test_lp": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderLengthPrefix}.Build(), ComponentCoderIds: []string{"test"}}.Build(),
				"test":    pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderRow}.Build()}.Build(),
			},
		},
		{"needLP_recurse",
			urns.CoderKV, []string{"k", "v"},
			map[string]*pipepb.Coder{},
			map[string]*pipepb.Coder{
				"k": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderRow}.Build()}.Build(),
				"v": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBool}.Build()}.Build(),
			},
			map[string]*pipepb.Coder{
				"test_lp": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderKV}.Build(), ComponentCoderIds: []string{"k_lp", "v"}}.Build(),
				"test":    pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderKV}.Build(), ComponentCoderIds: []string{"k", "v"}}.Build(),
				"k_lp":    pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderLengthPrefix}.Build(), ComponentCoderIds: []string{"k"}}.Build(),
				"k":       pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderRow}.Build()}.Build(),
				"v":       pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBool}.Build()}.Build(),
			},
		},
		{"alreadyLP", urns.CoderLengthPrefix, []string{"k"},
			map[string]*pipepb.Coder{},
			map[string]*pipepb.Coder{
				"k": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderRow}.Build()}.Build(),
			},
			map[string]*pipepb.Coder{
				"test": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderLengthPrefix}.Build(), ComponentCoderIds: []string{"k"}}.Build(),
				"k":    pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderRow}.Build()}.Build(),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Add the initial coder to base.
			test.base["test"] = pipepb.Coder_builder{
				Spec:              pipepb.FunctionSpec_builder{Urn: test.urn}.Build(),
				ComponentCoderIds: test.components,
			}.Build()

			lpUnknownCoders("test", test.bundle, test.base)

			if d := cmp.Diff(test.want, test.bundle, protocmp.Transform()); d != "" {
				t.Fatalf("lpUnknownCoders(%v); (-want, +got):\n%v", test.urn, d)
			}
		})
	}
}

func Test_reconcileCoders(t *testing.T) {
	tests := []struct {
		name         string
		bundle, base map[string]*pipepb.Coder
		want         map[string]*pipepb.Coder
	}{
		{name: "noChanges",
			bundle: map[string]*pipepb.Coder{
				"a": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBool}.Build()}.Build(),
			},
			base: map[string]*pipepb.Coder{
				"a": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBool}.Build()}.Build(),
				"b": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBytes}.Build()}.Build(),
				"c": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderStringUTF8}.Build()}.Build(),
			},
			want: map[string]*pipepb.Coder{
				"a": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBool}.Build()}.Build(),
			},
		},
		{name: "KV",
			bundle: map[string]*pipepb.Coder{
				"kv": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderKV}.Build(), ComponentCoderIds: []string{"k", "v"}}.Build(),
			},
			base: map[string]*pipepb.Coder{
				"kv": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderKV}.Build(), ComponentCoderIds: []string{"k", "v"}}.Build(),
				"k":  pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBool}.Build()}.Build(),
				"v":  pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBool}.Build()}.Build(),
			},
			want: map[string]*pipepb.Coder{
				"kv": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderKV}.Build(), ComponentCoderIds: []string{"k", "v"}}.Build(),
				"k":  pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBool}.Build()}.Build(),
				"v":  pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBool}.Build()}.Build(),
			},
		},
		{name: "KV-nested",
			bundle: map[string]*pipepb.Coder{
				"kv": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderKV}.Build(), ComponentCoderIds: []string{"k", "v"}}.Build(),
			},
			base: map[string]*pipepb.Coder{
				"kv": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderKV}.Build(), ComponentCoderIds: []string{"k", "v"}}.Build(),
				"k":  pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderKV}.Build(), ComponentCoderIds: []string{"a", "b"}}.Build(),
				"v":  pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBool}.Build()}.Build(),
				"a":  pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBytes}.Build()}.Build(),
				"b":  pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderRow}.Build()}.Build(),
				"c":  pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderStringUTF8}.Build()}.Build(),
			},
			want: map[string]*pipepb.Coder{
				"kv": pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderKV}.Build(), ComponentCoderIds: []string{"k", "v"}}.Build(),
				"k":  pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderKV}.Build(), ComponentCoderIds: []string{"a", "b"}}.Build(),
				"v":  pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBool}.Build()}.Build(),
				"a":  pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderBytes}.Build()}.Build(),
				"b":  pipepb.Coder_builder{Spec: pipepb.FunctionSpec_builder{Urn: urns.CoderRow}.Build()}.Build(),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reconcileCoders(test.bundle, test.base)

			if d := cmp.Diff(test.want, test.bundle, protocmp.Transform()); d != "" {
				t.Fatalf("reconcileCoders(...); (-want, +got):\n%v", d)
			}
		})
	}
}

func Test_pullDecoder(t *testing.T) {

	doubleBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(doubleBytes, math.Float64bits(math.SqrtPi))

	tests := []struct {
		name   string
		coder  *pipepb.Coder
		coders map[string]*pipepb.Coder
		input  []byte
	}{
		{
			"bytes",
			pipepb.Coder_builder{
				Spec: pipepb.FunctionSpec_builder{
					Urn: urns.CoderBytes,
				}.Build(),
			}.Build(),
			map[string]*pipepb.Coder{},
			[]byte{3, 1, 2, 3},
		}, {
			"varint",
			pipepb.Coder_builder{
				Spec: pipepb.FunctionSpec_builder{
					Urn: urns.CoderVarInt,
				}.Build(),
			}.Build(),
			map[string]*pipepb.Coder{},
			[]byte{255, 3},
		}, {
			"bool",
			pipepb.Coder_builder{
				Spec: pipepb.FunctionSpec_builder{
					Urn: urns.CoderBool,
				}.Build(),
			}.Build(),
			map[string]*pipepb.Coder{},
			[]byte{1},
		}, {
			"double",
			pipepb.Coder_builder{
				Spec: pipepb.FunctionSpec_builder{
					Urn: urns.CoderDouble,
				}.Build(),
			}.Build(),
			map[string]*pipepb.Coder{},
			doubleBytes,
		}, {
			"iterable",
			pipepb.Coder_builder{
				Spec: pipepb.FunctionSpec_builder{
					Urn: urns.CoderIterable,
				}.Build(),
				ComponentCoderIds: []string{"elm"},
			}.Build(),
			map[string]*pipepb.Coder{
				"elm": pipepb.Coder_builder{
					Spec: pipepb.FunctionSpec_builder{
						Urn: urns.CoderVarInt,
					}.Build(),
				}.Build(),
			},
			[]byte{4, 0, 1, 2, 3},
		}, {
			"kv",
			pipepb.Coder_builder{
				Spec: pipepb.FunctionSpec_builder{
					Urn: urns.CoderKV,
				}.Build(),
				ComponentCoderIds: []string{"key", "value"},
			}.Build(),
			map[string]*pipepb.Coder{
				"key": pipepb.Coder_builder{
					Spec: pipepb.FunctionSpec_builder{
						Urn: urns.CoderVarInt,
					}.Build(),
				}.Build(),
				"value": pipepb.Coder_builder{
					Spec: pipepb.FunctionSpec_builder{
						Urn: urns.CoderBool,
					}.Build(),
				}.Build(),
			},
			[]byte{3, 0},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dec := pullDecoder(test.coder, test.coders)
			buf := bytes.NewBuffer(test.input)
			got := dec(buf)
			if !bytes.EqualFold(test.input, got) {
				t.Fatalf("pullDecoder(%v)(...) = %v, want %v", test.coder, got, test.input)
			}
		})
	}
}
