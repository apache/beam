package beam

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/reflectx"
	"log"
)

// GroupByKey inserts a GBK transform into the pipeline.
func GroupByKey(p *Pipeline, a PCollection) (PCollection, error) {
	key, value, ok := reflectx.UnfoldKV(a.Type())
	if !ok {
		return PCollection{}, fmt.Errorf("Input type must by KV: %v", a)
	}

	// TODO(herohde): perhaps cleaner to not inject synthetic types, but
	// instead just manage the disjunction manually. Also, we cannot
	// distinguish between top-level []byte and Encoded.

	// (1) Create GBK result type.

	t, err := reflectx.MakeGBK(key, value)
	if err != nil {
		return PCollection{}, err
	}

	n := p.real.NewNode(t)

	// (2) Add GBK edge

	inT, _ := reflectx.MakeKV(reflectx.ByteSlice, value)
	outT, _ := reflectx.MakeGBK(reflectx.ByteSlice, value)

	log.Printf("GBK real: %v, in: %v, out: %v", t, inT, outT)

	edge := p.real.NewEdge(p.parent)
	edge.Op = graph.GBK
	edge.Input = []*graph.Inbound{{From: a.n, T: inT}}
	edge.Output = []*graph.Outbound{{To: n, T: outT}}

	log.Printf("GBK edge: %v", edge)
	return PCollection{n}, nil
}

func CoGroupByKey(p *Pipeline, cols ...PCollection) (PCollection, error) {
	return PCollection{}, nil
}
