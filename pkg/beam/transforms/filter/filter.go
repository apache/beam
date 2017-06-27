// Package filter contains transformations for removing pipeline elements based on
// various conditions.
package filter

import (
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/userfn"
	"github.com/apache/beam/sdks/go/pkg/beam/runtime/graphx"
)

var (
	sig = userfn.MakePredicate(typex.TType) // T -> bool
)

func init() {
	beam.RegisterType(reflect.TypeOf((*filterFn)(nil)).Elem())
}

// NOTE(herohde) 3/24/2017: the filter is an example of the user code being
// used inside a 'generic' DoFn. The encoded form would here be a poor choice.

// Filter filters the elements of a PCollection<A> based on the given function,
// which must be of the form: A -> bool. Filter removes all element for which
// the filter function returns false. It returns a PCollection of the same type
// as the input. For example:
//
//    words := beam.Create(p, "a", "b", "long", "alsolong")
//    short := filter.Filter(p, words, func(s string) bool {
//        return len(s) < 3
//    })
//
// Here, "short" will contain "a" and "b" at runtime.
func Filter(p *beam.Pipeline, col beam.PCollection, fn interface{}) beam.PCollection {
	p = p.Composite("filter.Filter")

	t := typex.SkipW(col.Type()).Type()
	userfn.MustSatisfy(fn, userfn.Replace(sig, typex.TType, t))

	return beam.ParDo(p, &filterFn{Filter: graphx.DataFnValue{Fn: reflect.ValueOf(fn)}}, col)
}

type filterFn struct {
	Filter graphx.DataFnValue `json:"filter"`
}

func (f *filterFn) ProcessElement(elm typex.T, emit func(typex.T)) {
	ret := f.Filter.Fn.Call([]reflect.Value{reflect.ValueOf(elm)})
	if ret[0].Bool() {
		emit(elm)
	}
}
