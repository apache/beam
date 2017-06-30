package stats

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
)

// Count counts the number of elements in a collection. It expects a
// PCollection<T> as input and returns a PCollection<KV<T,int>>. T's encoding
// must be a well-defined injection.
func Count(p *beam.Pipeline, col beam.PCollection) beam.PCollection {
	p = p.Composite("stats.Count")

	pre := beam.ParDo(p, mapFn, col)
	post := beam.GroupByKey(p, pre)
	return beam.ParDo(p, addFn, post)
}

// TODO: a real implementation would be less naive ..

func mapFn(elm typex.T) (typex.T, int) {
	return elm, 1
}

func addFn(key typex.T, counts func(*int) bool) (typex.T, int) {
	total := 0

	var v int
	for counts(&v) {
		total += v
	}
	return key, total
}

// Dedup removes all duplicates from a collection, under coder equality. It
// expects a PCollection<T> as input and returns a PCollection<T> with
// duplicates removed.
func Dedup(p *beam.Pipeline, col beam.PCollection) beam.PCollection {
	p = p.Composite("stats.DeDup")

	pre := beam.ParDo(p, mapFn, col)
	post := beam.GroupByKey(p, pre)
	return beam.ParDo(p, dedupFn, post)
}

func dedupFn(key typex.T, _ func(*int) bool) typex.T {
	return key
}
