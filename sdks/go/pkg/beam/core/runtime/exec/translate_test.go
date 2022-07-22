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

package exec

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestUnmarshalKeyedValues(t *testing.T) {
	tests := []struct {
		in  map[string]string
		exp []string
	}{
		{ // ordered
			map[string]string{"i0": "a", "i2": "c", "i1": "b"},
			[]string{"a", "b", "c"},
		},
		{ // ordered
			map[string]string{"i2": "c", "i1": "b", "i0": "a"},
			[]string{"a", "b", "c"},
		},
		{ // unordered fill in
			map[string]string{"i0": "a", "i2": "c", "foo": "b"},
			[]string{"a", "b", "c"},
		},
		{ // bogus
			map[string]string{"bogus": "b"},
			nil,
		},
		{ // out-of-bound also fill in (somewhat counter-intuitively in some cases)
			map[string]string{"i3": "a", "i2": "c", "i1": "b"},
			[]string{"a", "b", "c"},
		},
	}

	for _, test := range tests {
		actual := unmarshalKeyedValues(test.in)
		if !reflect.DeepEqual(actual, test.exp) {
			t.Errorf("unmarshalKeyedValues(%v) = %v, want %v", test.in, actual, test.exp)
		}
	}
}

func TestUnmarshalReshuffleCoders(t *testing.T) {
	payloads := map[string][]byte{}
	encode := func(id, urn string, comps ...string) {
		payloads[id] = protox.MustEncode(&pipepb.Coder{
			Spec: &pipepb.FunctionSpec{
				Urn: urn,
			},
			ComponentCoderIds: comps,
		})
	}
	encode("a", "beam:coder:bytes:v1")
	encode("b", "beam:coder:string_utf8:v1")
	encode("c", "beam:coder:kv:v1", "b", "a")

	got, err := unmarshalReshuffleCoders("c", payloads)
	if err != nil {
		t.Fatalf("unmarshalReshuffleCoders() err: %v", err)
	}

	want := coder.NewKV([]*coder.Coder{coder.NewString(), coder.NewBytes()})
	if !want.Equals(got) {
		t.Errorf("got %v, want != %v", got, want)
	}
}

func TestUnmarshallWindowFn(t *testing.T) {
	tests := []struct {
		name  string
		winFn *window.Fn
	}{
		{
			"global",
			window.NewGlobalWindows(),
		},
		{
			"interval",
			window.NewFixedWindows(5 * time.Minute),
		},
		{
			"sliding",
			window.NewSlidingWindows(time.Minute, 3*time.Minute),
		},
		{
			"sessions",
			window.NewSessions(10 * time.Minute),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec, err := makeWindowFn(test.winFn)
			if err != nil {
				t.Fatalf("failed to encode window function, got %v", err)
			}
			got, err := unmarshalWindowFn(spec)
			if err != nil {
				t.Fatalf("failed to unmarshal window fn, got %v", err)
			}
			if want := test.winFn; !want.Equals(got) {
				t.Errorf("got window fn %v, want %v", got, want)
			}
		})
	}
}

func TestUnmarshalWindowMapper(t *testing.T) {
	tests := []struct {
		name  string
		winFn *window.Fn
	}{
		{
			"global",
			window.NewGlobalWindows(),
		},
		{
			"interval",
			window.NewFixedWindows(5 * time.Minute),
		},
		{
			"sliding",
			window.NewSlidingWindows(time.Minute, 3*time.Minute),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec, err := makeWindowMappingFn(test.winFn)
			if err != nil {
				t.Fatalf("failed to encode window function, got %v", err)
			}
			wMap, err := unmarshalAndMakeWindowMapping(spec)
			if err != nil {
				t.Fatalf("failed to unmarshal window fn, got %v", err)
			}
			got, ok := wMap.(*windowMapper)
			if !ok {
				t.Fatalf("could not convert output to underlying windowMapper type, got %v", wMap)
			}
			if want := test.winFn; !want.Equals(got.wfn) {
				t.Errorf("got window fn %v, want %v", got, want)
			}
		})
	}
}

func makeWindowFn(w *window.Fn) (*pipepb.FunctionSpec, error) {
	switch w.Kind {
	case window.GlobalWindows:
		return &pipepb.FunctionSpec{
			Urn: graphx.URNGlobalWindowsWindowFn,
		}, nil
	case window.FixedWindows:
		return &pipepb.FunctionSpec{
			Urn: graphx.URNFixedWindowsWindowFn,
			Payload: protox.MustEncode(
				&pipepb.FixedWindowsPayload{
					Size: durationpb.New(w.Size),
				},
			),
		}, nil
	case window.SlidingWindows:
		return &pipepb.FunctionSpec{
			Urn: graphx.URNSlidingWindowsWindowFn,
			Payload: protox.MustEncode(
				&pipepb.SlidingWindowsPayload{
					Size:   durationpb.New(w.Size),
					Period: durationpb.New(w.Period),
				},
			),
		}, nil
	case window.Sessions:
		return &pipepb.FunctionSpec{
			Urn: graphx.URNSessionsWindowFn,
			Payload: protox.MustEncode(
				&pipepb.SessionWindowsPayload{
					GapSize: durationpb.New(w.Gap),
				},
			),
		}, nil
	default:
		return nil, errors.Errorf("unexpected windowing strategy: %v", w)
	}
}

func makeWindowMappingFn(w *window.Fn) (*pipepb.FunctionSpec, error) {
	wFn, err := makeWindowFn(w)
	if err != nil {
		return nil, err
	}
	switch w.Kind {
	case window.GlobalWindows:
		wFn.Urn = graphx.URNWindowMappingGlobal
	case window.FixedWindows:
		wFn.Urn = graphx.URNWindowMappingFixed
	case window.SlidingWindows:
		wFn.Urn = graphx.URNWindowMappingSliding
	default:
		return nil, fmt.Errorf("unknown window fn type %v", w.Kind)
	}
	return wFn, nil
}
