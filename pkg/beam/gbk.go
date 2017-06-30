package beam

import (
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
)

// GroupByKey is a PTransform that takes a PCollection of type W<KV<A,B>,
// groups the values by key and windows, and returns a PCollection of type
// W<GBK<A,B>> representing a map from each distinct key and window of the
// input PCollection to an iterable over all the values associated with
// that key in the input per window. Each key in the output PCollection is
// unique within each window.
//
// GroupByKey is analogous to converting a multi-map into a uni-map, and
// related to GROUP BY in SQL. It corresponds to the "shuffle" step between
// the Mapper and the Reducer in the MapReduce framework.
//
// Two keys of type A are compared for equality by first encoding each of the
// keys using the Coder of the keys of the input PCollection, and then
// comparing the encoded bytes. This admits efficient parallel evaluation.
// Note that this requires that the Coder of the keys be deterministic.
//
// By default, the Coder of the keys of the output PCollection is the same
// as that of the keys of the input, and the Coder of the elements of the
// iterable values of the output PCollection is the same as the Coder of the
// values of the input.
//
// Example of use:
//
//    urlDocPairs := ...
//    urlToDocs := beam.GroupByKey(p, urlDocPairs)
//    results := beam.ParDo(p, func (key string, values func(*Doc) bool) {
//          // ... process all docs having that url ...
//    }, urlToDocs)
//
// GroupByKey is a key primitive in data-parallel processing, since it is the
// main way to efficiently bring associated data together into one location.
// It is also a key determiner of the performance of a data-parallel pipeline.
//
// See CoGroupByKey for a way to group multiple input PCollections by a common
// key at once.
func GroupByKey(p *Pipeline, a PCollection) PCollection {
	return Must(TryGroupByKey(p, a))
}

// TODO(herohde) 5/30/2017: add windowing aspects to above documentation.
// TODO(herohde) 6/23/2017: support createWithFewKeys and other variants?

// TryGroupByKey inserts a GBK transform into the pipeline. Returns
// an error on failure.
func TryGroupByKey(p *Pipeline, a PCollection) (PCollection, error) {
	if !a.IsValid() {
		return PCollection{}, fmt.Errorf("invalid pcollection to GBK")
	}
	edge, err := graph.NewGBK(p.real, p.parent, a.n)
	if err != nil {
		return PCollection{}, err
	}
	ret := PCollection{edge.Output[0].To}
	ret.SetCoder(NewCoder(ret.Type()))
	return ret, nil
}

// CoGroupByKey inserts a CoGBK transform into the pipeline.
func CoGroupByKey(p *Pipeline, cols ...PCollection) PCollection {
	return Must(TryCoGroupByKey(p, cols...))
}

// TryCoGroupByKey inserts a CoGBK transform into the pipeline. Returns
// an error on failure.
func TryCoGroupByKey(p *Pipeline, cols ...PCollection) (PCollection, error) {
	panic("NYI")
}
