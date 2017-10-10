package stats

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
)

// Count counts the number of elements in a collection. It expects a
// PCollection<T> as input and returns a PCollection<KV<T,int>>. T's encoding
// must be a well-defined injection.
func Count(p *beam.Pipeline, col beam.PCollection) beam.PCollection {
	p = p.Composite("stats.Count")

	pre := beam.ParDo(p, mapFn, col)
	return Sum(p, pre)
}

func mapFn(elm beam.T) (beam.T, int) {
	return elm, 1
}
