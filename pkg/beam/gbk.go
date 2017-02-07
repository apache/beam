package beam

import (
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/reflectx"
	"log"
)

// groupByKey inserts a GBK transform into the pipeline.
func groupByKey(p *Pipeline, a PCollection) (PCollection, error) {
	if !a.IsValid() {
		return PCollection{}, Errorf(3, "Invalid pcollection to GBK")
	}

	key, value, ok := reflectx.UnfoldKV(a.Type())
	if !ok {
		return PCollection{}, Errorf(3, "Input type must by KV: %v", a)
	}

	// TODO(herohde): perhaps cleaner to not inject synthetic types, but
	// instead just manage the disjunction manually. Also, we cannot
	// distinguish between top-level []byte and Encoded.

	// (1) Create GBK result type.

	t, err := reflectx.MakeGBK(key, value)
	if err != nil {
		return PCollection{}, Errorf(3, "Bad GBK type: %v", err)
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
	return PCollection{n: n}, nil
}

// GroupByKey inserts a GBK transform into the pipeline.
func GroupByKey(p *Pipeline, a PCollection) PCollection {
	ret, err := groupByKey(p, a)
	if err != nil {
		p.errs.Add(err)
		return PCollection{}
	}
	return ret
}

func CoGroupByKey(p *Pipeline, cols ...PCollection) (PCollection, error) {
	return PCollection{}, nil
}
