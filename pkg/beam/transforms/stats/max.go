package stats

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

//go:generate specialize --input=max_switch.tmpl --x=integers,floats

// Max returns the maximal element -- per key, if keyed -- in a collection.
// It expects a PCollection<A> or PCollection<KV<A,B>> as input and returns
// a singleton PCollection<A> or a PCollection<KV<A,B>>, respectively. It
// can only be used for numbers, such as int, uint16, float32, etc.
//
// For example:
//
//    col := beam.Create(p, 1, 11, 7, 5, 10)
//    max := stats.Max(col)   // PCollection<int> with 11 as the only element.
//
func Max(p *beam.Pipeline, col beam.PCollection) beam.PCollection {
	p = p.Composite("stats.Max")

	var t reflect.Type
	switch {
	case typex.IsWKV(col.Type()):
		t = typex.SkipW(col.Type()).Components()[1].Type()
		col = beam.GroupByKey(p, col)

	case typex.IsWGBK(col.Type()):
		t = typex.SkipW(col.Type()).Components()[1].Type()

	default:
		t = typex.SkipW(col.Type()).Type()
	}

	if !reflectx.IsNumber(t) || reflectx.IsComplex(t) {
		panic(fmt.Sprintf("Max requires a non-complex number: %v", t))
	}

	// Do a pipeline-construction-time type switch to select the right
	// runtime operation.
	return maxSwitch(p, t, col)
}
