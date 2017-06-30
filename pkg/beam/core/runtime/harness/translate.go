package harness

import (
	"encoding/json"
	"errors"
	"fmt"

	protobuf "github.com/golang/protobuf/ptypes/any"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	fnapi_pb "github.com/apache/beam/sdks/go/pkg/beam/core/runtime/api/org_apache_beam_fn_v1"
	rnapi_pb "github.com/apache/beam/sdks/go/pkg/beam/core/runtime/api/org_apache_beam_runner_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
)

var (
	errRootlessBundle = errors.New("Invalid bundle: no roots supplied")
	errBundleHasCycle = errors.New("Bundle contained a cycle")
)

// Tracks provenance information of PCollections to help linking nodes
// to their predecessors.
type pCollInfo struct {
	xid   string                // constructing transform ID
	xform *rnapi_pb.PTransform  // constructing transform
	pcoll *rnapi_pb.PCollection // collection metadata
}

// lookups on PCollections by their ID.
type pCollMap map[string]*pCollInfo

type nodeID struct {
	StepID string
	Key    string
}

// topologicalSort produces a list of topologically sorted PTransform ids and
// a PCollection lookup structure for the supplied bundle. The function will
// fail if the graph has cycles.
func topologicalSort(bundle *fnapi_pb.ProcessBundleDescriptor) (sortedIds []string, colls pCollMap, err error) {
	colls = make(pCollMap)
	for id, coll := range bundle.GetPcollections() {
		colls[id] = &pCollInfo{pcoll: coll}
	}

	adjs := make(map[string]int)

	for id, transform := range bundle.GetTransforms() {
		// Populate the adjacency map
		in := len(transform.GetInputs())
		adjs[id] = in
		if in == 0 {
			// Root node identified.
			sortedIds = append(sortedIds, id)
		}
	}

	xforms := bundle.GetTransforms()
	if len(xforms) == 0 {
		return nil, nil, errRootlessBundle
	}

	frontier := append([]string(nil), sortedIds...)

	for {
		for _, id := range frontier {
			frontier = frontier[1:]
			xform := xforms[id]
			for _, out := range xform.GetOutputs() {
				// Look for consumer xforms that take this output as an input
				for cid, c := range xforms {
					for _, in := range c.GetInputs() {
						if in == out {
							// They are connected. Decrement the adjacency count of this xform
							adjs[cid] = adjs[cid] - 1
							// Update the PCollection metadata to record the producing transform.
							colls[in].xid, colls[in].xform = id, xforms[id]

							if adjs[cid] == 0 {
								// Add it to the list
								frontier = append(frontier, cid)
							}
						}
					}
				}
			}
		}
		// Add any completed nodes to the sorted list
		sortedIds = append(sortedIds, frontier...)

		// We're done when there are no more nodes to explore.
		if len(frontier) == 0 {
			break
		}
	}

	if len(sortedIds) != len(bundle.GetTransforms()) {
		return nil, nil, errBundleHasCycle
	}

	return sortedIds, colls, nil
}

// translate translates a ProcessBundleDescriptor to a sub-graph that can run bundles.
func translate(bundle *fnapi_pb.ProcessBundleDescriptor) (*graph.Graph, error) {
	// NOTE: we will see only graph fragments w/o GBK, IMPULSE or FLATTEN, which
	// are handled by the service.

	// The incoming bundle is an unsorted collection of data. By applying a topological sort
	// we can make a single linear pass to convert to the internal runner representation.
	sortedIds, colls, err := topologicalSort(bundle)
	if err != nil {
		return nil, err
	}

	coders, err := translateCoders(bundle.GetCodersyyy())
	if err != nil {
		return nil, fmt.Errorf("invalid coders: %v", err)
	}

	g := graph.New()
	nodes := make(map[nodeID]*graph.Node)
	xforms := bundle.GetTransforms()

	for _, id := range sortedIds {
		transform := xforms[id]
		spec := transform.GetSpec()
		//log.Printf("SPEC: %v %v", id, transform.GetSpec())
		switch spec.GetUrn() {
		case "urn:org.apache.beam:source:java:0.1": // using Java's for now.
			var me v1.MultiEdge
			if err := protox.UnpackProto(spec.GetParameter(), &me); err != nil {
				return nil, err
			}

			edge := g.NewEdge(g.Root())
			edge.Op = graph.Source
			edge.DoFn, edge.Input, edge.Output, err = graphx.DecodeMultiEdge(&me)
			if err != nil {
				return nil, err
			}
			if err := link(g, nodes, coders, transform, id, edge, colls); err != nil {
				return nil, err
			}

		case "urn:org.apache.beam:dofn:java:0.1": // We are using Java's for now.
			var me v1.MultiEdge
			if err := protox.UnpackBase64Proto(spec.GetParameter(), &me); err != nil {
				return nil, err
			}

			edge := g.NewEdge(g.Root())
			edge.Op = graph.ParDo
			edge.DoFn, edge.Input, edge.Output, err = graphx.DecodeMultiEdge(&me)
			if err != nil {
				return nil, err
			}
			if err := link(g, nodes, coders, transform, id, edge, colls); err != nil {
				return nil, err
			}

		case "urn:org.apache.beam:source:runner:0.1":
			port, err := translatePort(spec.GetParameter())
			if err != nil {
				return nil, err
			}

			if size := len(transform.GetOutputs()); size != 1 {
				return nil, fmt.Errorf("Expected 1 output, got %v", size)
			}
			var target *graph.Target
			for key := range transform.GetOutputs() {
				target = &graph.Target{ID: id, Name: key}
			}

			edge := g.NewEdge(g.Root())
			edge.Op = graph.DataSource
			edge.Port = port
			edge.Target = target
			edge.Output = []*graph.Outbound{{Type: nil}}

			if err := linkOutbound(g, nodes, coders, transform, id, edge, colls); err != nil {
				return nil, err
			}
			edge.Output[0].Type = edge.Output[0].To.Coder.T

		case "urn:org.apache.beam:sink:runner:0.1":
			port, err := translatePort(spec.GetParameter())
			if err != nil {
				return nil, err
			}

			if size := len(transform.GetInputs()); size != 1 {
				return nil, fmt.Errorf("Expected 1 input, got %v", size)
			}
			var target *graph.Target
			for key := range transform.GetInputs() {
				target = &graph.Target{ID: id, Name: key}
			}

			edge := g.NewEdge(g.Root())
			edge.Op = graph.DataSink
			edge.Port = port
			edge.Target = target
			edge.Input = []*graph.Inbound{{Type: nil}}

			if err := linkInbound(g, nodes, coders, transform, edge, colls); err != nil {
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
	var port fnapi_pb.RemoteGrpcPort
	if err := protox.Unpack(data, "type.googleapis.com/org.apache.beam.fn.v1.RemoteGrpcPort", &port); err != nil {
		return nil, err
	}
	return &graph.Port{
		ID:  port.GetApiServiceDescriptor().GetId(),
		URL: port.GetApiServiceDescriptor().GetUrl(),
	}, nil
}

func link(g *graph.Graph, nodes map[nodeID]*graph.Node, coders map[string]*coder.Coder, transform *rnapi_pb.PTransform, tid string, edge *graph.MultiEdge, colls pCollMap) error {
	if err := linkInbound(g, nodes, coders, transform, edge, colls); err != nil {
		return err
	}
	return linkOutbound(g, nodes, coders, transform, tid, edge, colls)
}

func linkInbound(g *graph.Graph, nodes map[nodeID]*graph.Node, coders map[string]*coder.Coder, transform *rnapi_pb.PTransform, edge *graph.MultiEdge, colls pCollMap) error {
	from := translateInputs(transform, colls)
	if len(from) != len(edge.Input) {
		return fmt.Errorf("unexpected number of inputs: %v, want %v", len(from), len(edge.Input))
	}
	for i := 0; i < len(edge.Input); i++ {
		edge.Input[i].From = nodes[from[i]]
	}
	return nil
}

func linkOutbound(g *graph.Graph, nodes map[nodeID]*graph.Node, coders map[string]*coder.Coder, transform *rnapi_pb.PTransform, tid string, edge *graph.MultiEdge, colls pCollMap) error {
	to := translateOutputs(transform, tid, colls)
	if len(to) != len(edge.Output) {
		return fmt.Errorf("unexpected number of outputs: %v, want %v", len(to), len(edge.Output))
	}

	w := window.NewGlobalWindow()
	if len(edge.Input) > 0 {
		w = edge.Input[0].From.Window()
	}
	for i := 0; i < len(edge.Output); i++ {
		c := coders[to[i].Coder]

		n := g.NewNode(c.T, w)
		n.Coder = c
		nodes[to[i].NodeID] = n

		edge.Output[i].To = n
	}
	return nil
}

func translateInputs(transform *rnapi_pb.PTransform, colls pCollMap) []nodeID {
	var from []nodeID

	for _, in := range transform.GetInputs() {
		// The runner API doesn't store the bidirectional relationship of nodes.
		// We identify the data by working backwards to the PCollection, then
		// consult our PCollection map to get info about the producing PTransform.
		// Since each PTransform may produce many outputs, we look at all of them
		// to find the output matching our input identifier.
		fid := colls[in].xid
		for okey, ocol := range colls[in].xform.GetOutputs() {
			if ocol == in {
				id := nodeID{fid, okey}
				from = append(from, id)
			}
		}
	}
	return from
}

type output struct {
	NodeID nodeID
	Coder  string
}

func translateOutputs(transform *rnapi_pb.PTransform, tid string, colls pCollMap) []output {
	var to []output

	for key, col := range transform.GetOutputs() {
		if key == "bogus" {
			continue // NOTE: remove bogus output
		}

		// TODO(herohde) 6/26/2017: we need to reorder output

		coder := colls[col].pcoll.GetCoderId()
		to = append(to, output{nodeID{tid, key}, coder})
	}

	return to
}

func translateCoders(in map[string]*rnapi_pb.Coder) (map[string]*coder.Coder, error) {
	coders := make(map[string]*coder.Coder)
	for id, coder := range in {
		spec := coder.GetSpec().GetSpec()
		c, err := unpackCoder(spec.GetParameter())
		if err != nil {
			return nil, fmt.Errorf("failed to translate coder %s: %v", id, err)
		}
		coders[id] = c
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
