package beam

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
)

// Flatten merges incoming PCollection<T>s to a single PCollection<T>.
func Flatten(p *Pipeline, cols ...PCollection) (PCollection, error) {
	for i, in := range cols {
		if !in.IsValid() {
			return PCollection{}, fmt.Errorf("invalid pcollection to flatten: index %v", i)
		}
	}
	if len(cols) == 0 {
		return PCollection{}, fmt.Errorf("no input pcollections")
	}
	if len(cols) == 1 {
		// Special case: flattening a single pcollection is a no-op.
		return cols[0], nil
	}

	var in []*graph.Node
	for _, s := range cols {
		in = append(in, s.n)
	}
	edge, err := graph.NewFlatten(p.real, p.parent, in)
	if err != nil {
		return PCollection{}, err
	}
	ret := PCollection{edge.Output[0].To}
	ret.SetCoder(NewCoder(ret.Type()))
	return ret, nil
}
