package beam

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"reflect"
)

// GroupByKey inserts a GBK transform into the pipeline.
func GroupByKey(p *Pipeline, a PCollection) (PCollection, error) {
	// TODO(herohde): if input is generic, we need to know the "real" underlying type.

	key, value, ok := graph.IsKV(a.Type())
	if !ok {
		return PCollection{}, fmt.Errorf("Input type must by KV: %v", a)
	}

	// TODO(herohde): perhaps cleaner to not inject synthetic types, but
	// instead just manage the disjunction manually.

	// (1) Create GBK result type.

	t := reflect.StructOf([]reflect.StructField{
		{Name: "Key", Tag: `beam:"key"`, Type: key},
		{Name: "Values", Tag: `beam:"values"`, Type: reflect.ChanOf(reflect.BothDir, value)},
	})

	n := p.real.NewNode(t)

	// (2) Add GBK edge

	edge := p.real.NewEdge(p.parent)
	edge.Op = graph.GBK
	edge.Input = []*graph.Inbound{{From: a.n}}
	edge.Output = []*graph.Outbound{{To: n}}

	return PCollection{n}, nil
}

func CoGroupByKey(p *Pipeline, cols ...PCollection) (PCollection, error) {
	return PCollection{}, nil
}
