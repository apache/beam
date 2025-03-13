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
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
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

func TestMayFixDataSourceCoder(t *testing.T) {
	knownStart := coder.NewW(
		coder.NewKV([]*coder.Coder{coder.NewBytes(), coder.NewI(coder.NewString())}),
		coder.NewGlobalWindow())
	knownWant := coder.NewW(
		coder.NewCoGBK([]*coder.Coder{coder.NewBytes(), coder.NewString()}),
		coder.NewGlobalWindow())

	makeParDo := func(t *testing.T, fn any) *ParDo {
		t.Helper()
		dfn, err := graph.NewDoFn(fn)
		if err != nil {
			t.Fatalf("couldn't construct ParDo with Sig: %T %v", fn, err)
		}
		return &ParDo{Fn: dfn}
	}

	tests := []struct {
		name        string
		start, want *coder.Coder
		out         Node
	}{
		{
			name:  "bytes",
			start: coder.NewBytes(),
		}, {
			name:  "W<bytes>",
			start: coder.NewW(coder.NewBytes(), coder.NewGlobalWindow()),
		}, {
			name: "W<KV<bytes,bool>",
			start: coder.NewW(
				coder.NewKV([]*coder.Coder{coder.NewBytes(), coder.NewBool()}),
				coder.NewGlobalWindow()),
		}, {
			name:  "W<KV<bytes,Iterable<string>>_nil",
			start: knownStart,
		}, {
			name:  "W<KV<bytes,Iterable<string>>_Expand",
			out:   &Expand{},
			start: knownStart,
			want:  knownWant,
		}, {
			name:  "W<KV<bytes,Iterable<string>>_Combine",
			out:   &Combine{},
			start: knownStart,
			want:  knownWant,
		}, {
			name:  "W<KV<bytes,Iterable<string>>_ReshuffleOutput",
			out:   &ReshuffleOutput{},
			start: knownStart,
			want:  knownWant,
		}, {
			name:  "W<KV<bytes,Iterable<string>>_MergeAccumulators",
			out:   &MergeAccumulators{},
			start: knownStart,
			want:  knownWant,
		}, {
			name:  "W<KV<bytes,Iterable<string>>_Multiplex_Expand",
			out:   &Multiplex{Out: []Node{&Expand{}}},
			start: knownStart,
			want:  knownWant,
		}, {
			name:  "W<KV<bytes,Iterable<string>>_Multiplex_ParDo_KV",
			out:   &Multiplex{Out: []Node{makeParDo(t, func([]byte, []string) {})}},
			start: knownStart,
		}, {
			name:  "W<KV<bytes,Iterable<string>>_Multiplex_ParDo_GBK",
			out:   &Multiplex{Out: []Node{makeParDo(t, func([]byte, func(*string) bool) {})}},
			start: knownStart,
			want:  knownWant,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// If want is nil, we expect no changes.
			if test.want == nil {
				test.want = test.start
			}

			u := &DataSource{
				Coder: test.start,
				Out:   test.out,
			}
			mayFixDataSourceCoder(u)
			if !test.want.Equals(u.Coder) {
				t.Errorf("mayFixDataSourceCoder(Datasource[Coder: %v, Out: %T]), got %v, want %v", test.start, test.out, u.Coder, test.want)
			}

		})
	}
}

func TestUnmarshalWindowFn(t *testing.T) {
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

func TestInputIdToIndex(t *testing.T) {
	tests := []struct {
		in   string
		want int
	}{
		{ // does not start with i
			"90",
			0,
		},
		{ // start with i
			"i0",
			0,
		},
		{
			"i1",
			1,
		},
		{
			"i10",
			10,
		},
	}

	for _, test := range tests {
		got, err := inputIdToIndex(test.in)
		if !strings.HasPrefix(test.in, "i") {
			if err == nil {
				t.Errorf("should return err when string does not has a prefix of i, but didn't. inputIdToIndex(%v) = (%v, %v)", test.in, got, err)
			}
		} else {
			if got != test.want {
				t.Errorf("can not correctly convert inputId to index. inputIdToIndex(%v) = (%v, %v), want %v", test.in, got, err, test.want)
			}
		}
	}
}

func TestIndexToInputId(t *testing.T) {
	tests := []struct {
		in   int
		want string
	}{
		{
			1,
			"i1",
		},
		{
			1000,
			"i1000",
		},
	}

	for _, test := range tests {
		got := indexToInputId(test.in)
		if got != test.want {
			t.Errorf("can not correctly convert index to inputId. indexToInputId(%v) = (%v), want %v", test.in, got, test.want)
		}
	}
}

func TestUnmarshalPort(t *testing.T) {
	var port fnpb.RemoteGrpcPort

	tests := []struct {
		inputData   []byte
		outputPort  Port
		outputStr   string
		outputError error
	}{
		{
			inputData:   []byte{},
			outputPort:  Port{URL: port.GetApiServiceDescriptor().GetUrl()},
			outputStr:   fnpb.RemoteGrpcPort{}.CoderId,
			outputError: nil,
		},
	}

	for _, test := range tests {
		port, str, err := unmarshalPort(test.inputData)
		if err != nil && test.outputError == nil {
			t.Errorf("there is an error where should not be. unmarshalPort(%v) = (%v, %v, %v), want (%v, %v, %v)", test.inputData, port, str, err, test.outputPort, test.outputStr, test.outputError)
		} else if err != nil && err != test.outputError {
			t.Errorf("got an unexpected error: %v, want: %v", err, test.outputError)
		} else if port != test.outputPort {
			t.Errorf("the output port is not right. unmarshalPort(%v) = (%v, %v, %v), want (%v, %v, %v)", test.inputData, port, str, err, test.outputPort, test.outputStr, test.outputError)
		} else if str != test.outputStr {
			t.Errorf("the output string is not right. unmarshalPort(%v) = (%v, %v, %v), want (%v, %v, %v)", test.inputData, port, str, err, test.outputPort, test.outputStr, test.outputError)
		}
	}
}

func TestUnmarshalPlan(t *testing.T) {
	transform := pipepb.PTransform{
		Spec: &pipepb.FunctionSpec{
			Urn: urnDataSource,
		},
		Outputs: map[string]string{},
	}
	tests := []struct {
		name        string
		inputDesc   *fnpb.ProcessBundleDescriptor
		outputPlan  *Plan
		outputError error
	}{
		{
			name: "test_no_root_units",
			inputDesc: &fnpb.ProcessBundleDescriptor{
				Id:         "",
				Transforms: map[string]*pipepb.PTransform{},
			},
			outputPlan:  nil,
			outputError: errors.New("no root units"),
		},
		{
			name: "test_zero_transform",
			inputDesc: &fnpb.ProcessBundleDescriptor{
				Id: "",
				Transforms: map[string]*pipepb.PTransform{
					"": {},
				},
			},
			outputPlan:  nil,
			outputError: errors.New("no root units"),
		},
		{
			name: "test_transform_outputs_length_not_one",
			inputDesc: &fnpb.ProcessBundleDescriptor{
				Id: "",
				Transforms: map[string]*pipepb.PTransform{
					"": &transform,
				},
			},
			outputPlan:  nil,
			outputError: errors.Errorf("expected one output from DataSource, got %v", transform.GetOutputs()),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := UnmarshalPlan(test.inputDesc, nil)
			if err != nil && test.outputError == nil {
				t.Errorf("there is an error where should not be. UnmarshalPlan(%v) = (%v, %v), want (%v, %v)", test.inputDesc, plan, err, test.outputPlan, test.outputError)
			} else if err != nil && !reflect.DeepEqual(err, test.outputError) {
				t.Errorf("got an unexpected error: %v, want: %v", err, test.outputError)
			} else if !reflect.DeepEqual(plan, test.outputPlan) {
				t.Errorf("the output builder is not right. UnmarshalPlan(%v) = (%v, %v), want (%v, %v)", test.inputDesc, plan, err, test.outputPlan, test.outputError)
			}
		})
	}
}

func TestNewBuilder(t *testing.T) {
	descriptor := fnpb.ProcessBundleDescriptor{
		Id:         "",
		Transforms: map[string]*pipepb.PTransform{},
	}
	tests := []struct {
		name          string
		inputDesc     *fnpb.ProcessBundleDescriptor
		outputBuilder *builder
		outputError   error
	}{
		{
			name:      "test_1",
			inputDesc: &descriptor,
			outputBuilder: &builder{
				desc:      &descriptor,
				coders:    graphx.NewCoderUnmarshaller(descriptor.GetCoders()),
				prev:      make(map[string]int),
				succ:      make(map[string][]linkID),
				windowing: make(map[string]*window.WindowingStrategy),
				nodes:     make(map[string]*PCollection),
				links:     make(map[linkID]Node),
				units:     nil,
				idgen:     &GenID{},
			},
			outputError: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b, err := newBuilder(test.inputDesc, nil)
			if err != nil && test.outputError == nil {
				t.Errorf("There is an error where should not be. newBuilder(%v) = (%v, %v), want (%v, %v)", test.inputDesc, b, err, test.outputBuilder, test.outputError)
			} else if err != nil && err != test.outputError {
				t.Errorf("got an unexpected error: %v, want: %v", err, test.outputError)
			} else if !reflect.DeepEqual(b, test.outputBuilder) {
				t.Errorf("The output builder is not right. newBuilder(%v) = (%v, %v), want (%v, %v)", test.inputDesc, b, err, test.outputBuilder, test.outputError)
			}
		})
	}
}
