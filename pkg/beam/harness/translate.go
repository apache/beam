package harness

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	protobuf "github.com/golang/protobuf/ptypes/any"
	protobufw "github.com/golang/protobuf/ptypes/wrappers"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/reflectx"
	pb "github.com/apache/beam/sdks/go/third_party/beam/org_apache_beam_fn_v1"
	"log"
	"reflect"
)

type nodeID struct {
	StepID string
	Key    string
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
func translateBundle(bundle *pb.ProcessBundleDescriptor) (*graph.Graph, error) {
	log.Printf("BUNDLE: %v", bundle)

	// coders := make(map[int64]*graph.Coder)
	// TODO: populate coders

	// NOTE: we rely on the transforms to be topologically ordered. Then we
	// can make a single pass.

	g := graph.New()
	nodes := make(map[nodeID]*graph.Node)

	for _, transform := range bundle.GetPrimitiveTransform() {
		edge := g.NewEdge(g.Root())

		// TODO: serialize opcode, types, I/O mapping, etc.
		spec := transform.GetFunctionSpec()
		edge.Op, _ = translateOpcode(spec.GetUrn())

		if spec.Data != nil {
			me, err := unpackData(spec.Data)
			if err != nil {
				return nil, err
			}

			if me.UserFn != nil {
				fn, err := graph.DecodeFnRef(me.UserFn)
				if err != nil {
					return nil, err
				}
				edge.DoFn, err = graph.ReflectFn(fn)
				if err != nil {
					return nil, err
				}

				if ct, ok := edge.DoFn.Context(); ok {
					f, _ := reflectx.FindTaggedField(ct, reflectx.DataTag)
					log.Printf("Data: %v of %v", me.Data, f.Type)

					data := reflect.New(f.Type).Interface()
					log.Printf("T: %v", reflect.TypeOf(data))

					if err := json.Unmarshal([]byte(me.Data), data); err != nil {
						return nil, fmt.Errorf("Failed to decode data: %v", err)
					}

					log.Printf("T2: %v %v", data, reflect.TypeOf(data))
					edge.Data = reflect.ValueOf(data).Elem().Interface()
				}

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

				var to []nodeID
				for key, _ := range transform.GetOutputs() {
					if key == "bogus" {
						continue // NOTE: remove bogus output
					}

					// TODO: real type and coder.
					// TODO: we need to reorder output

					to = append(to, nodeID{transform.GetId(), key})
				}
				for i := 0; i < len(me.Outbound); i++ {
					t, err := graph.DecodeType(me.Outbound[i].Type)
					if err != nil {
						return nil, fmt.Errorf("Failed to decode outbound: %v", err)
					}

					n := g.NewNode(t)
					n.Coder = beam.InternalCoder
					nodes[to[i]] = n

					output := &graph.Outbound{
						To: n,
						T:  t,
					}
					edge.Output = append(edge.Output, output)
				}
			}

		} else {
			// TODO: EXTERNAL
		}
	}

	return g, nil
}

func translateOpcode(urn string) (graph.Opcode, error) {
	switch urn {
	case "urn:org.apache.beam:source:java:0.1":
		return graph.Source, nil
	case "urn:org.apache.beam:dofn:java:0.1":
		return graph.ParDo, nil
	default:
		log.Printf("Unexpected opcode: %v", urn)
		return graph.External, nil // fmt.Errorf("Unexpected opcode: %v", urn)
	}
}

func unpackData(data *protobuf.Any) (*v1.MultiEdge, error) {
	var buf protobufw.BytesValue
	if err := proto.Unmarshal(data.Value, &buf); err != nil {
		return nil, fmt.Errorf("BytesValue unmarshal failed: %v", err)
	}

	// log.Printf("DATA: \"%v\"", string(buf.Value))

	decoded, err := base64.StdEncoding.DecodeString(string(buf.Value))
	if err != nil {
		// NOTE: for sources, the data is no longer base64 encoded. Bug?

		log.Printf("No base64: %v", err)
		decoded = buf.Value

		// return nil, fmt.Errorf("base64 decoding failed: %v", err)
	}

	var ret v1.MultiEdge
	if err := proto.Unmarshal(decoded, &ret); err != nil {
		return nil, fmt.Errorf("FunctionRef unmarshal failed: %v", err)
	}
	return &ret, nil
}
