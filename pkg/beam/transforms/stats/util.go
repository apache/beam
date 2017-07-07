package stats

import (
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

// makeCombinable prepares the given PCollection for a global or per-key combine of
// a concrete type. It inserts a GBK operation in the pipeline if given a KV,
// returning that new collection. Otherwise, the original collection is returned
// and no new steps are added to the pipeline. It also returns the element type.
func makeCombinable(p *beam.Pipeline, col beam.PCollection) (reflect.Type, beam.PCollection) {
	switch {
	case typex.IsWKV(col.Type()):
		upd := beam.GroupByKey(p, col)
		return typex.SkipW(col.Type()).Components()[1].Type(), upd

	case typex.IsWGBK(col.Type()):
		return typex.SkipW(col.Type()).Components()[1].Type(), col

	default:
		return typex.SkipW(col.Type()).Type(), col
	}
}
