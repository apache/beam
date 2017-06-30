// Package top contains transformations for finding the smallest (or largest) N
// elements based on arbitrary orderings.
package top

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

var (
	sig = funcx.MakePredicate(typex.TType, typex.TType) // (T, T) -> bool
)

func init() {
	beam.RegisterType(reflect.TypeOf((*combineFn)(nil)).Elem())
}

// Globally returns the top N elements of the incoming PCollection<T>, using the
// given comparator, less : T x T -> bool. It returns a single-element
// PCollection<[]T> with a slice of the N largest elements.
func Globally(p *beam.Pipeline, col beam.PCollection, n int, less interface{}) beam.PCollection {
	p = p.Composite(fmt.Sprintf("top.Globally(%v)", n))

	if n < 1 {
		panic(fmt.Sprintf("n must be > 0"))
	}
	t := typex.SkipW(col.Type()).Type()
	funcx.MustSatisfy(less, funcx.Replace(sig, typex.TType, t))

	return beam.Combine(p, &combineFn{Less: graphx.DataFnValue{Fn: reflect.ValueOf(less)}, N: n}, col)
}

// PerKey returns the top N elements for each key of the incoming PCollection<KV<A,B>>,
// using the given comparator, less : B x B -> bool. It returns a PCollection<KV<A,[]B>>
// with a single slice of the N largest elements for each key.
func PerKey(p *beam.Pipeline, col beam.PCollection, n int, less interface{}) beam.PCollection {
	p = p.Composite(fmt.Sprintf("top.PerKey(%v)", n))

	if n < 1 {
		panic(fmt.Sprintf("n must be > 0"))
	}
	if !typex.IsWKV(col.Type()) {
		panic(fmt.Sprintf("type must be a KV: %v", col.Type()))
	}

	t := typex.SkipW(col.Type()).Components()[1].Type()
	funcx.MustSatisfy(less, funcx.Replace(sig, typex.TType, t))

	keyed := beam.GroupByKey(p, col)
	return beam.Combine(p, &combineFn{Less: graphx.DataFnValue{Fn: reflect.ValueOf(less)}, N: n}, keyed)
}

// TODO(herohde) 5/25/2017: the accumulator should be serializable with a Coder.
// We need a coder here, because the elements are generally code-able only. Until
// then, we do not support combiner lifting.

// TODO(herohde) 5/25/2017: use a heap instead of a sorted slice.

type accum struct {
	// list stores the elements of type A in order. It has at most size N.
	list []reflect.Value
}

// combineFn is the internal CombineFn. It maintains accumulators containing
// sorted lists of element of the underlying type, A, up to size N, under the
// Less ordering on A.
type combineFn struct {
	// Less is the < order on the underlying type, A.
	Less graphx.DataFnValue `json:"less"`
	// N is the number of elements to keep.
	N int `json:"n"`
}

// TODO(herohde) 5/25/2017: a Setup/Init method would be useful.

func (f *combineFn) CreateAccumulator() accum {
	return accum{}
}

func (f *combineFn) AddInput(a accum, val typex.T) accum {
	t := f.Less.Fn.Type().In(0)                                  // == underlying type, A
	ret := append(a.list, exec.Convert(reflect.ValueOf(val), t)) // unwrap T
	return f.trim(ret)
}

func (f *combineFn) MergeAccumulators(list []accum) accum {
	var ret []reflect.Value
	for _, a := range list {
		ret = append(ret, a.list...)
	}
	return f.trim(ret)
}

func (f *combineFn) ExtractOutput(a accum) []typex.T {
	var ret []typex.T
	for _, elm := range a.list {
		ret = append(ret, elm.Interface()) // implicitly wrap T
	}
	return ret
}

func (f *combineFn) trim(ret []reflect.Value) accum {
	sort.SliceStable(ret, func(i, j int) bool {
		return f.Less.Fn.Call([]reflect.Value{ret[j], ret[i]})[0].Bool() // uses >
	})
	if len(ret) > f.N {
		ret = ret[:f.N]
	}
	return accum{list: ret}
}
