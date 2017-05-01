package harness

import (
	"encoding/json"
	"fmt"
	protobuf "github.com/golang/protobuf/ptypes/any"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/runtime/graphx/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/util/protox"
	pb "github.com/apache/beam/sdks/go/third_party/beam/org_apache_beam_fn_v1"
)

const (
	RemoteGrpcPortTypeUrl = "type.googleapis.com/org.apache.beam.fn.v1.RemoteGrpcPort"
)

// primitive_transform:<
//   id:"-14"
//   function_spec:<
//     id:"-11"
//     urn:"urn:org.apache.beam:dofn:java:0.1"
//     data:<
//       type_url:"type.googleapis.com/google.protobuf.BytesValue"
//       value:"\n\020bWFpbi5FeHRyYWN0"
//     >
//   >
//   inputs:<key:"-15" value:<target:<primitive_transform_reference:"-12" name:"-2" > > >
//   outputs:<key:"out" value:<coder_reference:"-5" > >
// >

// Output from GBK:
//
// primitive_transform: <
//   id: "-24"
//     function_spec: <
//       id: "-25"
//       urn: "urn:org.apache.beam:source:runner:0.1"
//       data: <
//           type_url: "type.googleapis.com/org.apache.beam.fn.v1.RemoteGrpcPort"
//           value: "\n\025\n\002-1\022\017localhost:46567"
//         >
//       >
//       inputs: <
//         key: "-22"
//         value: <
//         >
//       >
//       outputs: <
//         key: "-23"
//         value: <
//           coder_reference: "-30"
//         >
//       >
//     >
//
//    Coder -30: {
//       "@type": "kind:windowed_value",
//       "component_encodings":[
//           {"@type":"kind:pair",
//            "component_encodings":[
//                {"@type":"kind:length_prefix",
//                 "component_encodings":[{"@type":"CmUKMmdpdGh1Yi5jb20vZ29vZ2xlL2 [...]"}]},
//                {"@type":"kind:stream",
//                 "component_encodings":[
//                     {"@type": "kind:length_prefix",
//                      "component_encodings\":[{"@type":"CmUKMmdpdGh1Yi5jb20vZ29vZ2 [...]"}]}]}]},
//           {\"@type\":\"kind:global_window\"}]"
//    }

type nodeID struct {
	StepID string
	Key    string
}

// translate translates a ProcessBundleDescriptor to a sub-graph that can run bundles.
func translate(bundle *pb.ProcessBundleDescriptor) (*graph.Graph, error) {
	// log.Printf("BUNDLE: %v", bundle)

	coders, err := translateCoders(bundle.GetCoders())
	if err != nil {
		return nil, fmt.Errorf("invalid coders: %v", err)
	}

	// NOTE: we rely on the transforms to be topologically ordered. Then we
	// can make a single pass.

	g := graph.New()
	nodes := make(map[nodeID]*graph.Node)

	// NOTE: we will see only graph fragments w/o GBK or FLATTEN, which are handled by
	// the service.

	for _, transform := range bundle.GetPrimitiveTransform() {
		// log.Printf("SPEC: %v", transform.GetFunctionSpec())

		spec := transform.GetFunctionSpec()
		switch spec.GetUrn() {
		case "urn:org.apache.beam:source:java:0.1":
			var me v1.MultiEdge
			if err := protox.UnpackProto(spec.Data, &me); err != nil {
				return nil, err
			}

			edge := g.NewEdge(g.Root())
			edge.Op = graph.Source
			edge.DoFn, edge.Input, edge.Output, err = graphx.DecodeMultiEdge(&me)
			if err != nil {
				return nil, err
			}
			if err := link(g, nodes, coders, transform, edge); err != nil {
				return nil, err
			}

		case "urn:org.apache.beam:dofn:java:0.1":
			var me v1.MultiEdge
			if err := protox.UnpackBase64Proto(spec.Data, &me); err != nil {
				return nil, err
			}

			edge := g.NewEdge(g.Root())
			edge.Op = graph.ParDo
			edge.DoFn, edge.Input, edge.Output, err = graphx.DecodeMultiEdge(&me)
			if err != nil {
				return nil, err
			}
			if err := link(g, nodes, coders, transform, edge); err != nil {
				return nil, err
			}

		case "urn:org.apache.beam:source:runner:0.1":
			port, err := translatePort(spec.Data)
			if err != nil {
				return nil, err
			}

			if size := len(transform.GetInputs()); size != 1 {
				return nil, fmt.Errorf("Expected 1 input, got %v", size)
			}
			var target *graph.Target
			for key, _ := range transform.GetInputs() {
				target = &graph.Target{transform.GetId(), key}
			}

			edge := g.NewEdge(g.Root())
			edge.Op = graph.DataSource
			edge.Port = port
			edge.Target = target
			edge.Output = []*graph.Outbound{{Type: nil}}

			if err := linkOutbound(g, nodes, coders, transform, edge); err != nil {
				return nil, err
			}
			edge.Output[0].Type = edge.Output[0].To.Coder.T

		case "urn:org.apache.beam:sink:runner:0.1":
			port, err := translatePort(spec.Data)
			if err != nil {
				return nil, err
			}

			if size := len(transform.GetOutputs()); size != 1 {
				return nil, fmt.Errorf("Expected 1 output, got %v", size)
			}
			var target *graph.Target
			for key, _ := range transform.GetOutputs() {
				target = &graph.Target{transform.GetId(), key}
			}

			edge := g.NewEdge(g.Root())
			edge.Op = graph.DataSink
			edge.Port = port
			edge.Target = target
			edge.Input = []*graph.Inbound{{Type: nil}}

			if err := linkInbound(g, nodes, coders, transform, edge); err != nil {
				return nil, err
			}
			edge.Input[0].Type = edge.Input[0].From.Coder.T

		default:
			return nil, fmt.Errorf("Unexpected opcode: %v", spec)
		}
	}
	return g, nil
}

func translatePort(data *protobuf.Any) (*graph.Port, error) {
	var port pb.RemoteGrpcPort
	if err := protox.Unpack(data, RemoteGrpcPortTypeUrl, &port); err != nil {
		return nil, err
	}
	return &graph.Port{
		ID:  port.GetApiServiceDescriptor().GetId(),
		URL: port.GetApiServiceDescriptor().GetUrl(),
	}, nil
}

func link(g *graph.Graph, nodes map[nodeID]*graph.Node, coders map[string]*coder.Coder, transform *pb.PrimitiveTransform, edge *graph.MultiEdge) error {
	if err := linkInbound(g, nodes, coders, transform, edge); err != nil {
		return err
	}
	return linkOutbound(g, nodes, coders, transform, edge)
}

func linkInbound(g *graph.Graph, nodes map[nodeID]*graph.Node, coders map[string]*coder.Coder, transform *pb.PrimitiveTransform, edge *graph.MultiEdge) error {
	from := translateInputs(transform)
	if len(from) != len(edge.Input) {
		return fmt.Errorf("unexpected number of inputs: %v, want %v", len(from), len(edge.Input))
	}
	for i := 0; i < len(edge.Input); i++ {
		edge.Input[i].From = nodes[from[i]]
	}
	return nil
}

func linkOutbound(g *graph.Graph, nodes map[nodeID]*graph.Node, coders map[string]*coder.Coder, transform *pb.PrimitiveTransform, edge *graph.MultiEdge) error {
	to := translateOutputs(transform)
	if len(to) != len(edge.Output) {
		return fmt.Errorf("unexpected number of outputs: %v, want %v", len(to), len(edge.Output))
	}
	for i := 0; i < len(edge.Output); i++ {
		c := coders[to[i].Coder]

		n := g.NewNode(c.T)
		n.Coder = c
		nodes[to[i].NodeID] = n

		edge.Output[i].To = n
	}
	return nil
}

func translateInputs(transform *pb.PrimitiveTransform) []nodeID {
	var from []nodeID
	for _, in := range transform.GetInputs() {
		for _, target := range in.GetTarget() {
			// TODO: we need to reorder input to match. Multiplex?

			id := nodeID{target.GetPrimitiveTransformReference(), target.GetName()}
			from = append(from, id)
		}
	}
	return from
}

type output struct {
	NodeID nodeID
	Coder  string
}

func translateOutputs(transform *pb.PrimitiveTransform) []output {
	var to []output
	for key, col := range transform.GetOutputs() {
		if key == "bogus" {
			continue // NOTE: remove bogus output
		}

		// TODO: we need to reorder output

		coder := col.GetCoderReference()
		to = append(to, output{nodeID{transform.GetId(), key}, coder})
	}
	return to
}

func translateCoders(list []*pb.Coder) (map[string]*coder.Coder, error) {
	coders := make(map[string]*coder.Coder)
	for _, coder := range list {
		spec := coder.GetFunctionSpec()

		c, err := unpackCoder(spec.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to translate coder %s: %v", spec.GetId(), err)
		}
		coders[spec.GetId()] = c
	}
	return coders, nil
}

func unpackCoder(data *protobuf.Any) (*coder.Coder, error) {
	buf, err := protox.UnpackBytes(data)
	if err != nil {
		return nil, err
	}
	var c graphx.CoderRef
	if err := json.Unmarshal(buf, &c); err != nil {
		return nil, err
	}
	return graphx.DecodeCoder(&c)
}
