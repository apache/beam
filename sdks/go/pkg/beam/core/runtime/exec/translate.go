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
	"path"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/golang/protobuf/proto"
)

const (
	urnDataSource = "urn:org.apache.beam:source:runner:0.1"
	urnDataSink   = "urn:org.apache.beam:sink:runner:0.1"
)

// UnmarshalPlan converts a model bundle descriptor into an execution Plan.
func UnmarshalPlan(desc *fnpb.ProcessBundleDescriptor) (*Plan, error) {
	b, err := newBuilder(desc)
	if err != nil {
		return nil, err
	}
	for id, transform := range desc.GetTransforms() {
		if transform.GetSpec().GetUrn() != urnDataSource {
			continue
		}
		if len(transform.GetOutputs()) != 1 {
			return nil, fmt.Errorf("expected one output from DataSource, got %v", transform.GetOutputs())
		}

		port, cid, err := unmarshalPort(transform.GetSpec().GetPayload())
		if err != nil {
			return nil, err
		}

		u := &DataSource{UID: b.idgen.New(), Port: port}

		for key, pid := range transform.GetOutputs() {
			u.Target = Target{ID: id, Name: key}

			u.Out, err = b.makePCollection(pid)
			if err != nil {
				return nil, err
			}

			if cid == "" {
				u.Coder, err = b.makeCoderForPCollection(pid)
				if err != nil {
					return nil, err
				}
			} else {
				u.Coder, err = b.coders.Coder(cid) // Expected to be windowed coder
				if err != nil {
					return nil, err
				}
			}
		}

		b.units = append(b.units, u)
	}
	return b.build()
}

type builder struct {
	desc   *fnpb.ProcessBundleDescriptor
	coders *graphx.CoderUnmarshaller

	prev map[string]int      // PCollectionID -> #incoming
	succ map[string][]linkID // PCollectionID -> []linkID

	windowing map[string]*window.Window
	nodes     map[string]Node // PCollectionID -> Node (cache)
	links     map[linkID]Node // linkID -> Node (cache)

	units []Unit // result
	idgen *GenID
}

// linkID represents an incoming data link to an Node.
type linkID struct {
	to    string // TransformID
	input int    // input index. If > 0, it's a side input.
}

func newBuilder(desc *fnpb.ProcessBundleDescriptor) (*builder, error) {
	// Preprocess graph structure to allow insertion of Multiplex,
	// Flatten and Discard.

	prev := make(map[string]int)      // PCollectionID -> #incoming
	succ := make(map[string][]linkID) // PCollectionID -> []linkID

	for id, transform := range desc.GetTransforms() {
		if len(transform.GetSubtransforms()) > 0 {
			continue // ignore composites
		}

		input := unmarshalKeyedValues(transform.GetInputs())
		for i, from := range input {
			succ[from] = append(succ[from], linkID{id, i})
		}
		output := unmarshalKeyedValues(transform.GetOutputs())
		for _, to := range output {
			prev[to]++
		}
	}

	b := &builder{
		desc:   desc,
		coders: graphx.NewCoderUnmarshaller(desc.GetCoders()),

		prev: prev,
		succ: succ,

		windowing: make(map[string]*window.Window),
		nodes:     make(map[string]Node),
		links:     make(map[linkID]Node),

		idgen: &GenID{},
	}
	return b, nil
}

func (b *builder) build() (*Plan, error) {
	return NewPlan(b.desc.GetId(), b.units)
}

func (b *builder) makeWindow(id string) (*window.Window, error) {
	if w, exists := b.windowing[id]; exists {
		return w, nil
	}

	ws, ok := b.desc.GetWindowingStrategies()[id]
	if !ok {
		return nil, fmt.Errorf("windowing strategy %v not found", id)
	}
	if urn := ws.GetWindowFn().GetSpec().GetUrn(); urn != graphx.URNGlobalWindowsWindowFn {
		return nil, fmt.Errorf("unsupported window type: %v", urn)
	}

	w := window.NewGlobalWindow()
	b.windowing[id] = w
	return w, nil
}

func (b *builder) makePCollections(out []string) ([]Node, error) {
	var ret []Node
	for _, o := range out {
		n, err := b.makePCollection(o)
		if err != nil {
			return nil, err
		}
		ret = append(ret, n)
	}
	return ret, nil
}

func (b *builder) makeCoderForPCollection(id string) (*coder.Coder, error) {
	col, ok := b.desc.GetPcollections()[id]
	if !ok {
		return nil, fmt.Errorf("pcollection %v not found", id)
	}
	c, err := b.coders.Coder(col.CoderId)
	if err != nil {
		return nil, err
	}
	// TODO(herohde) 3/16/2018: remove potential WindowedValue from Dataflow.
	return coder.SkipW(c), nil
}

func (b *builder) makePCollection(id string) (Node, error) {
	if n, exists := b.nodes[id]; exists {
		return n, nil
	}

	list := b.succ[id]

	var u Node
	switch len(list) {
	case 0:
		// Discard.

		u = &Discard{UID: b.idgen.New()}

	case 1:
		return b.makeLink(id, list[0])

	default:
		// Multiplex.

		out, err := b.makeLinks(id, list)
		if err != nil {
			return nil, err
		}
		u = &Multiplex{UID: b.idgen.New(), Out: out}
	}

	if count := b.prev[id]; count > 1 {
		// Guard node with Flatten, if needed.

		b.units = append(b.units, u)
		u = &Flatten{UID: b.idgen.New(), N: count, Out: u}
	}

	b.nodes[id] = u
	b.units = append(b.units, u)
	return u, nil
}

func (b *builder) makeLinks(from string, ids []linkID) ([]Node, error) {
	var ret []Node
	for _, id := range ids {
		n, err := b.makeLink(from, id)
		if err != nil {
			return nil, err
		}
		ret = append(ret, n)
	}
	return ret, nil
}

func (b *builder) makeLink(from string, id linkID) (Node, error) {
	if n, ok := b.links[id]; ok {
		return n, nil
	}

	// Process all incoming links for the edge and cache them. It thus doesn't matter
	// which exact link triggers the Node generation. The link caching is only needed
	// to process ParDo side inputs.

	transform := b.desc.GetTransforms()[id.to]
	urn := transform.GetSpec().GetUrn()
	payload := transform.GetSpec().GetPayload()

	// TODO(herohde) 1/25/2018: do we need to handle composites?

	out, err := b.makePCollections(unmarshalKeyedValues(transform.GetOutputs()))
	if err != nil {
		return nil, err
	}

	var u Node
	switch urn {
	case graphx.URNParDo, graphx.URNJavaDoFn:
		var data string
		if urn == graphx.URNParDo {
			var pardo pb.ParDoPayload
			if err := proto.Unmarshal(payload, &pardo); err != nil {
				return nil, fmt.Errorf("invalid ParDo payload for %v: %v", transform, err)
			}
			data = string(pardo.GetDoFn().GetSpec().GetPayload())
		} else {
			// TODO(herohde) 12/4/2017: we see DoFns directly with Dataflow. Handle that
			// case here, for now, so that the harness can use this logic.

			data = string(payload)
		}

		// TODO(herohde) 1/28/2018: Once we're fully off the old way,
		// we can simply switch on the ParDo DoFn URN directly.

		var tp v1.TransformPayload
		if err := protox.DecodeBase64(data, &tp); err != nil {
			return nil, fmt.Errorf("invalid transform payload for %v: %v", transform, err)
		}

		switch tp.GetUrn() {
		case graphx.URNDoFn:
			op, fn, in, _, err := graphx.DecodeMultiEdge(tp.GetEdge())
			if err != nil {
				return nil, err
			}

			switch op {
			case graph.ParDo:
				n := &ParDo{UID: b.idgen.New(), PID: id.to, Inbound: in, Out: out}
				n.Fn, err = graph.AsDoFn(fn)
				if err != nil {
					return nil, err
				}
				// TODO(lostluck): 2018/03/22 Look into why transform.UniqueName isn't populated at this point, and switch n.PID to that instead.
				n.PID = path.Base(n.Fn.Name())
				if len(in) == 1 {
					u = n
					break
				}

				panic("NYI: side input")

			case graph.Combine:
				n := &Combine{UID: b.idgen.New(), Out: out[0]}
				n.Fn, err = graph.AsCombineFn(fn)
				if err != nil {
					return nil, err
				}

				// TODO(herohde) 6/28/2017: maybe record the per-key mode in the Edge
				// instead of inferring it here?

				c, err := b.makeCoderForPCollection(from)
				if err != nil {
					return nil, err
				}

				n.IsPerKey = coder.IsCoGBK(c)
				n.UsesKey = typex.IsKV(in[0].Type)

				u = n

			default:
				panic(fmt.Sprintf("Opcode should be one of ParDo or Combine, but it is: %v", op))
			}

		case graphx.URNInject:
			c, err := b.makeCoderForPCollection(from)
			if err != nil {
				return nil, err
			}
			if !coder.IsKV(c) {
				return nil, fmt.Errorf("unexpected inject coder: %v", c)
			}
			u = &Inject{UID: b.idgen.New(), N: (int)(tp.Inject.N), ValueEncoder: MakeElementEncoder(c.Components[1]), Out: out[0]}

		case graphx.URNExpand:
			var pid string
			for _, id := range transform.GetOutputs() {
				pid = id
			}
			c, err := b.makeCoderForPCollection(pid)
			if err != nil {
				return nil, err
			}
			if !coder.IsCoGBK(c) {
				return nil, fmt.Errorf("unexpected expand coder: %v", c)
			}

			var decoders []ElementDecoder
			for _, dc := range c.Components[1:] {
				decoders = append(decoders, MakeElementDecoder(dc))
			}
			u = &Expand{UID: b.idgen.New(), ValueDecoders: decoders, Out: out[0]}

		default:
			return nil, fmt.Errorf("unexpected payload: %v", tp)
		}

	case urnDataSink:
		port, cid, err := unmarshalPort(payload)
		if err != nil {
			return nil, err
		}

		sink := &DataSink{UID: b.idgen.New(), Port: port}

		for key, pid := range transform.GetInputs() {
			sink.Target = Target{ID: id.to, Name: key}

			if cid == "" {
				sink.Coder, err = b.makeCoderForPCollection(pid)
				if err != nil {
					return nil, err
				}
			} else {
				sink.Coder, err = b.coders.Coder(cid) // Expected to be windowed coder
				if err != nil {
					return nil, err
				}
			}
		}
		u = sink

	default:
		panic(fmt.Sprintf("Unexpected transform URN: %v", urn))
	}

	b.links[id] = u
	b.units = append(b.units, u)
	return u, nil
}

// unmarshalKeyedValues converts a map {"i1": "b", ""i0": "a"} into an ordered list of
// of values: {"a", "b"}. If the keys are not in the expected format, the returned
// list does not guarantee any order.
func unmarshalKeyedValues(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}

	// (1) Compute index. If generated by the marshaller, we have
	// a "iN" name that directly indicates the position.

	index := make(map[string]int)
	complete := true

	for key, _ := range m {
		if i, err := strconv.Atoi(strings.TrimPrefix(key, "i")); !strings.HasPrefix(key, "i") || err != nil {
			complete = false
			break
		} else {
			index[key] = i
		}
	}

	// (2) Impose order, if present, on values.

	if !complete {
		// Inserted node or fallback. Assume any order is ok.
		var ret []string
		for key, value := range m {
			if key == "bogus" {
				continue // Ignore special bogus node for legacy Dataflow.
			}
			ret = append(ret, value)
		}
		return ret
	}

	ret := make([]string, len(m))
	for key, value := range m {
		ret[index[key]] = value
	}
	return ret
}

func unmarshalPort(data []byte) (Port, string, error) {
	var port fnpb.RemoteGrpcPort
	if err := proto.Unmarshal(data, &port); err != nil {
		return Port{}, "", err
	}
	return Port{
		URL: port.GetApiServiceDescriptor().GetUrl(),
	}, port.CoderId, nil
}
