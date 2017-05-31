package filter

import (
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/runtime/graphx"
)

func init() {
	graphx.Register(reflect.TypeOf((*filterFn)(nil)).Elem())
}

// NOTE(herohde) 3/24/2017: the filter is an example of the user code being
// used inside a 'generic' DoFn. The encoded form would here be a poor choice.

// Filter filters the elements based on the given function, which must be
// of the form: T -> bool.
func Filter(p *beam.Pipeline, col beam.PCollection, fn interface{}) beam.PCollection {
	p = p.Composite("filter.Filter")

	// TODO: validate signature of fn
	return beam.ParDo(p, &filterFn{Filter: graphx.DataFnValue{Fn: reflect.ValueOf(fn)}}, col)
}

type filterFn struct {
	Filter graphx.DataFnValue `json:"filter"`
	fn     reflect.Value
}

func (f *filterFn) Setup() {
	f.fn = reflect.ValueOf(f.Filter.Fn)
}

func (f *filterFn) ProcessElement(elm typex.T, emit func(typex.T)) {
	ret := f.fn.Call([]reflect.Value{reflect.ValueOf(elm)})
	if ret[0].Bool() {
		emit(elm)
	}
}
