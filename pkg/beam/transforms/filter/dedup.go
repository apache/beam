package filter

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
)

// Dedup removes all duplicates from a collection, under coder equality. It
// expects a PCollection<T> as input and returns a PCollection<T> with
// duplicates removed.
func Dedup(p *beam.Pipeline, col beam.PCollection) beam.PCollection {
	p = p.Scope("filter.Dedup")

	pre := beam.ParDo(p, mapFn, col)
	post := beam.GroupByKey(p, pre)
	return beam.ParDo(p, keyFn, post)
}

func mapFn(elm beam.T) (beam.T, int) {
	return elm, 1
}

func keyFn(key beam.T, _ func(*int) bool) beam.T {
	return key
}
