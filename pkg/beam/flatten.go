package beam

import (
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

// Flatten is a PTransform that takes either multiple PCollections of type 'A'
// and returns a single PCollection of type 'A' containing all the elements in
// all the input PCollections; or a single PCollection<[]A> and returns a
// PCollection<A> containing all the elements for each incoming slice. The name
// "Flatten" suggests taking a list of lists and flattening them into a single
// list.
//
// Example of use:
//
//    a := textio.Read(p, "...")
//    b := textio.Read(p, "...")
//    c := textio.Read(p, "...")
//    merged := beam.Flatten(p, a, b, c)
//
//    d := top.Top(p, merged, 5, ...)    // PCollection<[]A>
//    top5 := beam.Flatten(p, d)
//
// By default, the Coder of the output PCollection is the same as the Coder
// of the first PCollection.
func Flatten(p *Pipeline, cols ...PCollection) PCollection {
	return Must(TryFlatten(p, cols...))
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
		// Special case: flattening a single pcollection is either a list
		// flatten or a no-op.
		if typex.IsList(typex.SkipW(cols[0].Type()).Type()) {
			return ParDo(p, flattenListFn, cols[0]), nil
		}
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

func flattenListFn(list []typex.T, emit func(typex.T)) {
	for _, elm := range list {
		emit(elm)
	}
}
