package filter

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"reflect"
)

// NOTE(herohde) 3/24/2017: the filter is an example of the user code being
// used inside a 'generic' DoFn. The encoded form would here be a poor choice.

// Filter filters the elements based on the given function, which must be
// of the form: T -> bool.
func Filter(p *beam.Pipeline, col beam.PCollection, fn interface{}) (beam.PCollection, error) {
	p = p.Composite("filter.Filter")

	// TODO: validate signature of fn
	return beam.ParDo(p, filterFn, col, beam.Data{DataFnValue{fn}})
}

type filterOpt struct {
	Fn DataFnValue `beam:"opt"`
}

func filterFn(opt filterOpt, elm typex.T, emit func(typex.T)) {
	fn := reflect.ValueOf(opt.Fn.Fn)
	ret := fn.Call([]reflect.Value{reflect.ValueOf(elm)})
	if ret[0].Bool() {
		emit(elm)
	}
}
