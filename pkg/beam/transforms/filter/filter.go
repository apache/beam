// Package filter contains transformations for removing pipeline elements based on
// various conditions.
package filter

import (
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

var (
	sig = funcx.MakePredicate(typex.TType) // T -> bool
)

func init() {
	beam.RegisterType(reflect.TypeOf((*filterFn)(nil)).Elem())
}

// Include filters the elements of a PCollection<A> based on the given function,
// which must be of the form: A -> bool. Include removes all element for which
// the filter function returns false. It returns a PCollection of the same type
// as the input. For example:
//
//    words := beam.Create(p, "a", "b", "long", "alsolong")
//    short := filter.Include(p, words, func(s string) bool {
//        return len(s) < 3
//    })
//
// Here, "short" will contain "a" and "b" at runtime.
func Include(p *beam.Pipeline, col beam.PCollection, fn interface{}) beam.PCollection {
	p = p.Composite("filter.Include")

	t := typex.SkipW(col.Type()).Type()
	funcx.MustSatisfy(fn, funcx.Replace(sig, typex.TType, t))

	return beam.ParDo(p, &filterFn{Predicate: graphx.DataFnValue{Fn: reflect.ValueOf(fn)}, Include: true}, col)
}

// Exclude filters the elements of a PCollection<A> based on the given function,
// which must be of the form: A -> bool. Exclude removes all element for which
// the filter function returns true. It returns a PCollection of the same type
// as the input. For example:
//
//    words := beam.Create(p, "a", "b", "long", "alsolong")
//    long := filter.Exclude(p, words, func(s string) bool {
//        return len(s) < 3
//    })
//
// Here, "long" will contain "long" and "alsolong" at runtime.
func Exclude(p *beam.Pipeline, col beam.PCollection, fn interface{}) beam.PCollection {
	p = p.Composite("filter.Exclude")

	t := typex.SkipW(col.Type()).Type()
	funcx.MustSatisfy(fn, funcx.Replace(sig, typex.TType, t))

	return beam.ParDo(p, &filterFn{Predicate: graphx.DataFnValue{Fn: reflect.ValueOf(fn)}, Include: false}, col)
}

type filterFn struct {
	// Filter is the predicate
	Predicate graphx.DataFnValue `json:"predicate"`
	// Include indicates whether to include or exclude elements that satisfy the predicate.
	Include bool `json:"include"`
}

func (f *filterFn) ProcessElement(elm typex.T, emit func(typex.T)) {
	ret := f.Predicate.Fn.Call([]reflect.Value{reflect.ValueOf(elm)})
	if ret[0].Bool() == f.Include {
		emit(elm)
	}
}
