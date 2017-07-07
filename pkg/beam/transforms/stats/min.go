package stats

import (
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

//go:generate specialize --input=min_switch.tmpl --x=integers,floats

// Min returns the minimal element -- per key, if keyed -- in a collection.
// It expects a PCollection<A> or PCollection<KV<A,B>> as input and returns
// a singleton PCollection<A> or a PCollection<KV<A,B>>, respectively. It
// can only be used for numbers, such as int, uint16, float32, etc.
//
// For example:
//
//    col := beam.Create(p, 1, 11, 7, 5, 10)
//    min := stats.Min(col)   // PCollection<int> with 1 as the only element.
//
func Min(p *beam.Pipeline, col beam.PCollection) beam.PCollection {
	p = p.Composite("stats.Min")

	t, prepped := makeCombinable(p, col)
	if !reflectx.IsNumber(t) || reflectx.IsComplex(t) {
		panic(fmt.Sprintf("Min requires a non-complex number: %v", t))
	}

	// Do a pipeline-construction-time type switch to select the right
	// runtime operation.
	return minSwitch(p, t, prepped)
}
