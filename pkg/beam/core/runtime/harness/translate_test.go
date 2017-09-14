package harness

import (
	"reflect"

	google_protobuf "github.com/golang/protobuf/ptypes/any"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	fnapi_pb "github.com/apache/beam/sdks/go/pkg/beam/core/runtime/api/org_apache_beam_fn_v1"
	rnapi_pb "github.com/apache/beam/sdks/go/pkg/beam/core/runtime/api/org_apache_beam_runner_v1"
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

func init() {
	runtime.RegisterType(reflect.TypeOf((*emitLinesFn)(nil)).Elem())
	runtime.SymbolResolver = fakeSymbols
}

func createReferenceGraph() *fnapi_pb.ProcessBundleDescriptor {
	// See https://goo.gl/Mm6N19 for instructions to capture a worker
	// graph to use as a seed for test data.
	return &fnapi_pb.ProcessBundleDescriptor{
		Id: "-7",
		Transforms: map[string]*rnapi_pb.PTransform{
			"-14": &rnapi_pb.PTransform{
				Spec: &rnapi_pb.FunctionSpec{
					Urn: "urn:org.apache.beam:source:java:0.1",
					AnyParam: &google_protobuf.Any{
						TypeUrl: "type.googleapis.com/google.protobuf.BytesValue",
						Value:   []byte("\na\nO\022\023\010\030\022\017\010\032J\013emitLinesFn\0328{\"lines\":[\"old pond\",\"a frog leaps in\",\"water's sound\"]}\032\016\n\014\n\004\010\031@\016\022\004\n\002\010\014"),
					},
				},
				Outputs: map[string]string{"-6": "-9"},
			},
			"-17": &rnapi_pb.PTransform{
				Spec: &rnapi_pb.FunctionSpec{
					Urn: "urn:org.apache.beam:dofn:java:0.1",
					AnyParam: &google_protobuf.Any{
						TypeUrl: "type.googleapis.com/google.protobuf.BytesValue",
						Value:   []byte("\n\300\001ClkKVwpBZ2l0aHViLmNvbS9nb29nbGUvZ28tYmVhbS1zZGstZGV2L3BrZy9iZWFtL3RyYW5zZm9ybXMvY291bnQubWFwRm4SEggWIgQIGUAPKgQIGUAPKgIIAhISCAESDgoECBlADhIGCgQIGUAPGh4KHAoECBlADhIUCgQIGUALEgYKBAgZQA8SBAoCCAI="),
					},
				},
				Inputs:  map[string]string{"-16": "-11"},
				Outputs: map[string]string{"out": "-13"},
			},
			"-20": &rnapi_pb.PTransform{
				Spec: &rnapi_pb.FunctionSpec{
					Urn: "urn:org.apache.beam:dofn:java:0.1",
					AnyParam: &google_protobuf.Any{
						TypeUrl: "type.googleapis.com/google.protobuf.BytesValue",
						Value:   []byte("\n`CiIKIAoObWFpbi5leHRyYWN0Rm4SDggWIgIIDCIGCBYiAggMEhAIARIMCgQIGUAOEgQKAggMGg4KDAoECBlADhIECgIIDA=="),
					},
				},
				Inputs:  map[string]string{"-19": "-9"},
				Outputs: map[string]string{"out": "-11"},
			},
			"-4": &rnapi_pb.PTransform{
				Spec: &rnapi_pb.FunctionSpec{
					Urn: "urn:org.apache.beam:sink:runner:0.1",
					AnyParam: &google_protobuf.Any{
						TypeUrl: "type.googleapis.com/org.apache.beam.fn.v1.RemoteGrpcPort",
						Value:   []byte("\n\025\n\002-1\022\017localhost:36335"),
					},
				},
				Inputs: map[string]string{"-3": "-13"},
			},
		},
		Pcollections: map[string]*rnapi_pb.PCollection{
			"-11": &rnapi_pb.PCollection{CoderId: "-10"},
			"-13": &rnapi_pb.PCollection{CoderId: "-12"},
			"-9":  &rnapi_pb.PCollection{CoderId: "-8"},
		},
		Coders: map[string]*rnapi_pb.Coder{
			"-10": &rnapi_pb.Coder{
				Spec: &rnapi_pb.SdkFunctionSpec{
					Spec: &rnapi_pb.FunctionSpec{
						AnyParam: &google_protobuf.Any{
							TypeUrl: "type.googleapis.com/google.protobuf.BytesValue",
							Value:   []byte("\n\202\003{\"component_encodings\":[{\"@type\":\"kind:length_prefix\",\"component_encodings\":[{\"@type\":\"CgRqc29uEgIIDBpMCjJnaXRodWIuY29tL2dvb2dsZS9nby1iZWFtLXNkay1kZXYvcGtnL2JlYW0uanNvbkVuYxIWCBYiBAgZQA8qBggUEgIICCoECBlAASJSCjJnaXRodWIuY29tL2dvb2dsZS9nby1iZWFtLXNkay1kZXYvcGtnL2JlYW0uanNvbkRlYxIcCBYiBAgZQAMiBggUEgIICCoECBlADyoECBlAAQ==\"}]},{\"@type\":\"kind:global_window\"}],\"@type\":\"kind:windowed_value\"}"),
						},
					},
				},
			},
			"-12": &rnapi_pb.Coder{
				Spec: &rnapi_pb.SdkFunctionSpec{
					Spec: &rnapi_pb.FunctionSpec{
						AnyParam: &google_protobuf.Any{
							TypeUrl: "type.googleapis.com/google.protobuf.BytesValue",
							Value:   []byte("\n\334\005{\"@type\":\"kind:windowed_value\",\"component_encodings\":[{\"@type\":\"kind:pair\",\"component_encodings\":[{\"@type\":\"kind:length_prefix\",\"component_encodings\":[{\"@type\":\"CgRqc29uEgIIDBpMCjJnaXRodWIuY29tL2dvb2dsZS9nby1iZWFtLXNkay1kZXYvcGtnL2JlYW0uanNvbkVuYxIWCBYiBAgZQA8qBggUEgIICCoECBlAASJSCjJnaXRodWIuY29tL2dvb2dsZS9nby1iZWFtLXNkay1kZXYvcGtnL2JlYW0uanNvbkRlYxIcCBYiBAgZQAMiBggUEgIICCoECBlADyoECBlAAQ==\"}]},{\"@type\":\"kind:length_prefix\",\"component_encodings\":[{\"@type\":\"CgRqc29uEgIIAhpMCjJnaXRodWIuY29tL2dvb2dsZS9nby1iZWFtLXNkay1kZXYvcGtnL2JlYW0uanNvbkVuYxIWCBYiBAgZQA8qBggUEgIICCoECBlAASJSCjJnaXRodWIuY29tL2dvb2dsZS9nby1iZWFtLXNkay1kZXYvcGtnL2JlYW0uanNvbkRlYxIcCBYiBAgZQAMiBggUEgIICCoECBlADyoECBlAAQ==\"}]}]},{\"@type\":\"kind:global_window\"}]}"),
						},
					},
				},
			},
			"-8": &rnapi_pb.Coder{
				Spec: &rnapi_pb.SdkFunctionSpec{
					Spec: &rnapi_pb.FunctionSpec{
						AnyParam: &google_protobuf.Any{
							TypeUrl: "type.googleapis.com/google.protobuf.BytesValue",
							Value:   []byte("\n\202\003{\"@type\":\"kind:windowed_value\",\"component_encodings\":[{\"component_encodings\":[{\"@type\":\"CgRqc29uEgIIDBpMCjJnaXRodWIuY29tL2dvb2dsZS9nby1iZWFtLXNkay1kZXYvcGtnL2JlYW0uanNvbkVuYxIWCBYiBAgZQA8qBggUEgIICCoECBlAASJSCjJnaXRodWIuY29tL2dvb2dsZS9nby1iZWFtLXNkay1kZXYvcGtnL2JlYW0uanNvbkRlYxIcCBYiBAgZQAMiBggUEgIICCoECBlADyoECBlAAQ==\"}],\"@type\":\"kind:length_prefix\"},{\"@type\":\"kind:global_window\"}]}"),
						},
					},
				},
			},
		},
	}
}

// TODO(herohde) 7/6/2017: these test are extremely painful to maintain. Improve or kill.

/*
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

func TestTranslateCoders(t *testing.T) {
	// Check translateCoders call
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
*/
