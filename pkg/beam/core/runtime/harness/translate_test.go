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

package harness

import (
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	fnapi_pb "github.com/apache/beam/sdks/go/pkg/beam/core/runtime/api/fnexecution_v1"
	rnapi_pb "github.com/apache/beam/sdks/go/pkg/beam/core/runtime/api/pipeline_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
)

// Declare the DoFn that is used in the test graph
type emitLinesFn struct {
	Lines []string `json:"lines"`
}

func (e *emitLinesFn) ProcessElement(emit func(string)) {
}

// Create a fake symbol lookup. For this test, it only needs
// to return the address of a valid function. The function never
// actually gets called. We make sure that remains true by supplying
// a function that only panics.

func panicer() {
	panic("Nothing should call this.")
}

type symlookup bool

func (s symlookup) Sym2Addr(name string) (uintptr, error) {
	return reflect.ValueOf(panicer).Pointer(), nil
}

var fakeSymbols symlookup

// retain the key for the registered function to encode into the test execution plan.
var emitLinesFnKey string

func init() {
	emitLinesFnKey = runtime.RegisterType(reflect.TypeOf((*emitLinesFn)(nil)).Elem())
	runtime.SymbolResolver = fakeSymbols
}

func windowedString() *v1.FullType {
	return &v1.FullType{
		Type: &v1.Type{
			Kind:    v1.Type_SPECIAL,
			Special: v1.Type_WINDOWEDVALUE,
		},
		Components: []*v1.FullType{
			&v1.FullType{
				Type: &v1.Type{
					Kind: v1.Type_STRING,
				},
			},
		},
	}
}

func makeDoFn() []byte {
	// This is a schematic for a DoFn that takes a windowed string
	// and produces a windowed string.
	me := v1.MultiEdge{
		Opcode: "ParDo",
		Inbound: []*v1.MultiEdge_Inbound{
			&v1.MultiEdge_Inbound{
				Kind: v1.MultiEdge_Inbound_MAIN,
				Type: windowedString(),
			},
		},
		Outbound: []*v1.MultiEdge_Outbound{
			&v1.MultiEdge_Outbound{
				Type: windowedString(),
			},
		},
		// schematic of a function that takes two arguments, an input string, and an output emitter of string.
		Fn: &v1.Fn{
			Fn: &v1.UserFn{
				Name: "main.doFn",
				Type: &v1.Type{
					Kind: v1.Type_FUNC,
					ParameterTypes: []*v1.Type{
						&v1.Type{Kind: v1.Type_STRING},
						&v1.Type{
							Kind: v1.Type_FUNC,
							ParameterTypes: []*v1.Type{
								&v1.Type{Kind: v1.Type_STRING},
							},
						},
					},
				},
			},
		},
	}

	res, err := protox.EncodeBase64(&me)
	if err != nil {
		panic(err)
	}
	return []byte(res)
}

func makeSource() []byte {
	me := v1.MultiEdge{
		Outbound: []*v1.MultiEdge_Outbound{
			&v1.MultiEdge_Outbound{
				Type: windowedString(),
			},
		},
		Fn: &v1.Fn{
			Opt: "{\"lines\":[\"old pond\",\"a frog leaps in\",\"water's sound\"]}",
			Type: &v1.Type{
				Kind: v1.Type_PTR,
				Element: &v1.Type{
					Kind:        v1.Type_EXTERNAL,
					ExternalKey: emitLinesFnKey,
				},
				ExternalKey: emitLinesFnKey,
			},
		},
	}

	res, err := protox.EncodeBase64(&me)
	if err != nil {
		panic(err)
	}
	return []byte(res)
}

func createReferenceGraph() *fnapi_pb.ProcessBundleDescriptor {
	return &fnapi_pb.ProcessBundleDescriptor{
		Id: "-7",
		Transforms: map[string]*rnapi_pb.PTransform{
			"-14": &rnapi_pb.PTransform{
				Spec: &rnapi_pb.FunctionSpec{
					Urn:     "urn:org.apache.beam:source:java:0.1",
					Payload: makeSource(),
				},
				Outputs: map[string]string{"-6": "-9"},
			},
			"-17": &rnapi_pb.PTransform{
				Spec: &rnapi_pb.FunctionSpec{
					Urn:     "urn:beam:dofn:javasdk:0.1",
					Payload: makeDoFn(),
				},
				Inputs:  map[string]string{"-16": "-11"},
				Outputs: map[string]string{"out": "-13"},
			},
			"-20": &rnapi_pb.PTransform{
				Spec: &rnapi_pb.FunctionSpec{
					Urn:     "urn:beam:dofn:javasdk:0.1",
					Payload: makeDoFn(),
				},
				Inputs:  map[string]string{"-19": "-9"},
				Outputs: map[string]string{"out": "-11"},
			},
			"-4": &rnapi_pb.PTransform{
				Spec: &rnapi_pb.FunctionSpec{
					Urn:     "urn:org.apache.beam:sink:runner:0.1",
					Payload: []byte("\n\025\n\002-1\022\017localhost:36335"),
				},
				Inputs: map[string]string{"-3": "-13"},
			},
		},
		Pcollections: map[string]*rnapi_pb.PCollection{
			"-11": &rnapi_pb.PCollection{CoderId: "-10"},
			"-13": &rnapi_pb.PCollection{CoderId: "-10"},
			"-9":  &rnapi_pb.PCollection{CoderId: "-10"},
		},
		Coders: getCoders(),
	}
}

func TestGraphTranslationSuccess(t *testing.T) {
	_, err := translate(createReferenceGraph())
	if err != nil {
		t.Errorf("translation failed: %v", err)
	}
}

func TestGraphCycleDetection(t *testing.T) {
	g := createReferenceGraph()
	// Introduce a cycle in the graph
	g.Transforms["-14"].Inputs = map[string]string{"-50": "-52"}
	g.Transforms["-4"].Outputs = map[string]string{"-51": "-52"}
	_, err := translate(g)
	if err == nil {
		t.Errorf("got nil error, expected cycle error: %v", g)
	}
}

func TestTooManyOutputsSource(t *testing.T) {
	g := createReferenceGraph()
	g.Transforms["-14"].Outputs["-51"] = "-11"
	_, err := translate(g)
	if err == nil {
		t.Errorf("got nil error, expected bad source error: %v", g)
	}
}

func getCoders() map[string]*rnapi_pb.Coder {
	return map[string]*rnapi_pb.Coder{
		"-12": &rnapi_pb.Coder{
			Spec: &rnapi_pb.SdkFunctionSpec{
				Spec: &rnapi_pb.FunctionSpec{
					// TODO(wcn): this blob will go away once coders are implemented as components rather than monoliths.
					Payload: []byte("{\"@type\":\"kind:windowed_value\",\"component_encodings\":[{\"@type\":\"kind:pair\",\"component_encodings\":[{\"component_encodings\":[{\"@type\":\"CgRqc29uEgIIAhpMCjJnaXRodWIuY29tL2dvb2dsZS9nby1iZWFtLXNkay1kZXYvcGtnL2JlYW0uSlNPTkVuYxIWCBYiBAgZQA8qBggUEgIICCoECBlAASJSCjJnaXRodWIuY29tL2dvb2dsZS9nby1iZWFtLXNkay1kZXYvcGtnL2JlYW0uSlNPTkRlYxIcCBYiBAgZQAMiBggUEgIICCoECBlADyoECBlAAQ==\"}],\"@type\":\"kind:length_prefix\"},{\"@type\":\"kind:length_prefix\",\"component_encodings\":[{\"@type\":\"CgRqc29uEgIIAhpMCjJnaXRodWIuY29tL2dvb2dsZS9nby1iZWFtLXNkay1kZXYvcGtnL2JlYW0uSlNPTkVuYxIWCBYiBAgZQA8qBggUEgIICCoECBlAASJSCjJnaXRodWIuY29tL2dvb2dsZS9nby1iZWFtLXNkay1kZXYvcGtnL2JlYW0uSlNPTkRlYxIcCBYiBAgZQAMiBggUEgIICCoECBlADyoECBlAAQ==\"}]}]},{\"@type\":\"kind:global_window\"}]}"),
				},
			},
		},
		"ByteArrayCoder": &rnapi_pb.Coder{
			Spec: &rnapi_pb.SdkFunctionSpec{
				Spec: &rnapi_pb.FunctionSpec{
					Urn: "urn:beam:coders:bytes:0.1",
				},
			},
		},
		"Coder": &rnapi_pb.Coder{
			Spec: &rnapi_pb.SdkFunctionSpec{
				Spec: &rnapi_pb.FunctionSpec{
					Urn: "urn:beam:coders:global_window:0.1",
				},
			},
		},
		"-10": &rnapi_pb.Coder{
			Spec: &rnapi_pb.SdkFunctionSpec{
				Spec: &rnapi_pb.FunctionSpec{
					Urn: "urn:beam:coders:windowed_value:0.1",
				},
			},
			ComponentCoderIds: []string{
				"ByteArrayCoder",
				"Coder",
			},
		},
	}
}

func TestTranslateCoders(t *testing.T) {
	input := getCoders()
	coders, err := translateCoders(input)
	if err != nil {
		t.Errorf("translateCoders() failed: %v", err)
	}

	expected := len(input) - 1 // We don't see the global window coder
	if len(coders) != expected {
		t.Errorf("Got %d coders, wanted %d.", len(coders), expected)
	}
}

func TestUnknownOpcode(t *testing.T) {
	// Check unexpected opcode
}

func TestTooManyInputsSink(t *testing.T) {
	g := createReferenceGraph()
	g.Transforms["-4"].Inputs["-51"] = "-11"
	_, err := translate(g)
	if err == nil {
		t.Errorf("got nil error, expected bad sink error: %v", g)
	}
}

func TestBundleHasNoRoots(t *testing.T) {
	g := createReferenceGraph()
	// Remove all the transforms
	g.Transforms = make(map[string]*rnapi_pb.PTransform)

	_, err := translate(g)
	if err != errRootlessBundle {
		t.Errorf("got %v, wanted %v in graph: %v", err, errRootlessBundle, g)
	}
}
