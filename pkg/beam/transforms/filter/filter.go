package filter

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"reflect"
)

// NOTE(herohde) 3/24/2017: the filter is an example of the user code being
// used inside a 'generic' DoFn. The encoded form would here be a poor choice.

type filterOpt struct {
	Fn graph.DataFnValue `beam:"data"`
}

func filterFn(opt filterOpt, in <-chan reflect.Value, out chan<- reflect.Value) error {
	fn := reflect.ValueOf(opt.Fn.Fn)
	for elm := range in {
		ret := fn.Call([]reflect.Value{elm})
		if ret[0].Bool() {
			out <- elm
		}
	}
	return nil
}

// Filter filters the elements based on the given function, which must be
// of the form: T -> bool.
func Filter(p *beam.Pipeline, col beam.PCollection, fn interface{}) beam.PCollection {
	p = p.Composite("filter.Filter")

	// TODO: validate signature of fn

	return beam.ParDo(p, filterFn, col, beam.Data{graph.DataFnValue{fn}})
}
