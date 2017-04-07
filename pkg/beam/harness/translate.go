package harness

import (
	"context"
	"encoding/json"
	"fmt"
	protobuf "github.com/golang/protobuf/ptypes/any"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/protox"
	"github.com/apache/beam/sdks/go/pkg/beam/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/typex"
	pb "github.com/apache/beam/sdks/go/third_party/beam/org_apache_beam_fn_v1"
	"log"
)

const (
	RemoteGrpcPortTypeUrl = "type.googleapis.com/org.apache.beam.fn.v1.RemoteGrpcPort"
)

type nodeID struct {
	StepID string
	Key    string
}

type output struct {
	NodeID nodeID
	Coder  string
}

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

// translateBundle translates a ProcessBundleDescriptor to a sub-graph that can run
// bundles.
func translateBundle(ctx context.Context, mgr *DataConnectionManager, bundle *pb.ProcessBundleDescriptor) (*graph.Graph, error) {
	log.Printf("BUNDLE: %v", bundle)

	coders := make(map[string]*graph.Coder)
	for _, coder := range bundle.Coders {
		spec := coder.GetFunctionSpec()

		c, err := translateCoder(spec.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to translate coder %s: %v", spec.GetId(), err)
		}
		coders[spec.GetId()] = c
	}

	// NOTE: we rely on the transforms to be topologically ordered. Then we
	// can make a single pass.

	g := graph.New()
	nodes := make(map[nodeID]*graph.Node)

	for _, transform := range bundle.GetPrimitiveTransform() {
		// TODO: serialize opcode, types, I/O mapping, etc.
		// NOTE: we will see only graph fragments w/o GBK or FLATTEN, which are handled by
		// the service.

		spec := transform.GetFunctionSpec()
		switch spec.GetUrn() {
		case "urn:org.apache.beam:source:java:0.1":
			var me v1.MultiEdge
			if err := protox.UnpackProto(spec.Data, &me); err != nil {
				return nil, err
			}
			dofn, data, err := decodeFn(me.UserFn, me.Data)
			if err != nil {
				return nil, err
			}

			edge := g.NewEdge(g.Root())
			edge.Op = graph.Source
			edge.DoFn = dofn
			edge.Data = data

			to := translateOutputs(transform)
			for i := 0; i < len(me.Outbound); i++ {
				t, err := graph.DecodeType(me.Outbound[i].Type)
				if err != nil {
					return nil, fmt.Errorf("Failed to decode outbound: %v", err)
				}
				real, err := graph.DecodeType(me.Outbound[i].Node.Type)
				if err != nil {
					return nil, fmt.Errorf("Failed to decode node: %v", err)
				}

				n := g.NewNode(real)
				n.Coder = coders[to[i].Coder]
				nodes[to[i].NodeID] = n

				output := &graph.Outbound{
					To: n,
					T:  t,
				}
				edge.Output = append(edge.Output, output)
			}

		case "urn:org.apache.beam:dofn:java:0.1":
			var me v1.MultiEdge
			if err := protox.UnpackBase64Proto(spec.Data, &me); err != nil {
				return nil, err
			}
			dofn, data, err := decodeFn(me.UserFn, me.Data)
			if err != nil {
				return nil, err
			}

			edge := g.NewEdge(g.Root())
			edge.Op = graph.ParDo
			edge.DoFn = dofn
			edge.Data = data

			var from []*graph.Node
			for _ /* key */, in := range transform.GetInputs() {
				for _, target := range in.GetTarget() {
					// TODO: we need to reorder input
					n := nodes[nodeID{target.GetPrimitiveTransformReference(), target.GetName()}]
					from = append(from, n)
				}
			}
			for i := 0; i < len(me.Inbound); i++ {
				t, err := graph.DecodeType(me.Inbound[i].Type)
				if err != nil {
					return nil, fmt.Errorf("Failed to decode inbound: %v", err)
				}

				input := &graph.Inbound{
					From: from[i],
					T:    t,
				}
				edge.Input = append(edge.Input, input)
			}

			to := translateOutputs(transform)
			for i := 0; i < len(me.Outbound); i++ {
				t, err := graph.DecodeType(me.Outbound[i].Type)
				if err != nil {
					return nil, fmt.Errorf("Failed to decode outbound: %v", err)
				}
				real, err := graph.DecodeType(me.Outbound[i].Node.Type)
				if err != nil {
					return nil, fmt.Errorf("Failed to decode node: %v", err)
				}

				n := g.NewNode(real)
				n.Coder = coders[to[i].Coder]
				nodes[to[i].NodeID] = n

				output := &graph.Outbound{
					To: n,
					T:  t,
				}
				edge.Output = append(edge.Output, output)
			}

		case "urn:org.apache.beam:sink:runner:0.1":
			var port pb.RemoteGrpcPort
			if err := protox.Unpack(spec.Data, RemoteGrpcPortTypeUrl, &port); err != nil {
				return nil, err
			}
			if size := len(transform.GetOutputs()); size != 1 {
				return nil, fmt.Errorf("Expected 1 output, got %v", size)
			}

			portID := port.GetApiServiceDescriptor().GetId()
			portUrl := port.GetApiServiceDescriptor().GetUrl()
			if err := mgr.Open(ctx, portID, portUrl); err != nil {
				return nil, fmt.Errorf("Failed to open data port %v: %v", portUrl, err)
			}

			var target *pb.Target
			for key, _ := range transform.GetOutputs() {
				target = &pb.Target{transform.GetId(), key}
			}

			// NOTE: we use the Encoded type as input to receive serialized data of type n.T.
			// For now, all we need is to wrap pipeline data into the global window.

			edge := g.NewEdge(g.Root())
			edge.Op = graph.External

			for _ /* key */, in := range transform.GetInputs() {
				for _, target := range in.GetTarget() {
					n := nodes[nodeID{target.GetPrimitiveTransformReference(), target.GetName()}]
					edge.Input = append(edge.Input, &graph.Inbound{n, reflectx.T})
				}
			}

			n := edge.Input[0].From
			edge.DoFn, _ = graph.ReflectFn(func(opt DataConnectionContext, in <-chan typex.T) error {
				return SinkFn(mgr, portID, n.Coder, n.T, opt, target, in)
			})

		default:
			return nil, fmt.Errorf("Unexpected opcode: %v", spec)
		}
	}

	return g, nil
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

func translateCoder(data *protobuf.Any) (*graph.Coder, error) {
	buf, err := protox.UnpackBytes(data)
	if err != nil {
		return nil, err
	}
	var c graph.CoderRef
	if err := json.Unmarshal(buf, &c); err != nil {
		return nil, err
	}
	return graph.DecodeCoder(&c)
}

func decodeFn(ref *v1.FunctionRef, dataStr string) (*graph.UserFn, interface{}, error) {
	dofn, err := graph.DecodeAndReflectFnRef(ref)
	if err != nil {
		return nil, nil, err
	}
	data, err := graph.DecodeData(dofn, dataStr)
	if err != nil {
		return nil, nil, err
	}
	return dofn, data, nil
}
