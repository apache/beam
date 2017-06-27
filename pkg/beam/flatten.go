package beam

import (
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
)

// Flatten is a PTransform that takes multiple PCollections of type 'A' and
// returns a single PCollection of type 'A' containing all the elements in
// all the input PCollections. The name "Flatten" suggests taking a list of
// lists and flattening them into a single list.
//
// Example of use:
//
//    a := textio.Read(p, "...")
//    b := textio.Read(p, "...")
//    c := textio.Read(p, "...")
//    merged := beam.Flatten(p, a, b, c)
//
// By default, the Coder of the output PCollection is the same as the Coder
// of the first PCollection.
func Flatten(p *Pipeline, cols ...PCollection) PCollection {
	return Must(TryFlatten(p, cols...))
}

// FlattenCol expects a PCollection<[]A> and returns a PCollection<A>
// containing all the elements for each incoming slice.
func FlattenCol(p *Pipeline, col PCollection) PCollection {
	return ParDo(p, flattenColFn, col)
}

func flattenColFn(list []typex.T, emit func(typex.T)) {
	for _, elm := range list {
		emit(elm)
	}
}

// TryFlatten merges incoming PCollections of type 'A' to a single PCollection
// of type 'A'. Returns an error indicating the set of PCollections that could
// not be flattened.
func TryFlatten(p *Pipeline, cols ...PCollection) (PCollection, error) {
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
	ret.SetCoder(cols[0].Coder())
	return ret, nil
}
