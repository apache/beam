package count

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
)

// PerElement counts the number of elements in a collection by key.
func PerElement(p *beam.Pipeline, col beam.PCollection) (beam.PCollection, error) {
	p = p.Composite("count.PerElement")

	pre, err := beam.ParDo(p, mapFn, col)
	if err != nil {
		return beam.PCollection{}, err
	}
	post, err := beam.GroupByKey(p, pre)
	if err != nil {
		return beam.PCollection{}, err
	}
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

// Dedup removes all duplicates from a collection, under coder equality.
func Dedup(p *beam.Pipeline, col beam.PCollection) (beam.PCollection, error) {
	p = p.Composite("count.DeDup")

	pre, err := beam.ParDo(p, mapFn, col)
	if err != nil {
		return beam.PCollection{}, err
	}
	post, err := beam.GroupByKey(p, pre)
	if err != nil {
		return beam.PCollection{}, err
	}
	return beam.ParDo(p, dedupFn, post)
}

func dedupFn(key typex.T, _ func(*int) bool) typex.T {
	return key
}
