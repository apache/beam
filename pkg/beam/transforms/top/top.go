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
)

var (
	sig = funcx.MakePredicate(beam.TType, beam.TType) // (T, T) -> bool
)

func init() {
	beam.RegisterType(reflect.TypeOf((*combineFn)(nil)).Elem())
}

// Largest returns the largest N elements either globally or per-key of the
// incoming PCollection, depending on its type:
//
//    PCollection<T>        : globally
//    PCollection<KV<K,T>>  : per-key
//    PCollection<GBK<K,T>> : per-key
//
// The order is defined by the comparator, less : T x T -> bool. It returns
// a single-element PCollection<[]T> or PCollection<K,[]T> with a slice of the
// N largest elements either globally or per key.
//
// Example use:
//
//    col := beam.Create(p, 1, 11, 7, 5, 10)
//    top2 := stats.Largest(p, col, 2, less)  // PCollection<[]int> with [11, 10] as the only element.
//
func Largest(p *beam.Pipeline, col beam.PCollection, n int, less interface{}) beam.PCollection {
	p = p.Composite(fmt.Sprintf("top.Largest(%v)", n))

	if n < 1 {
		panic(fmt.Sprintf("n must be > 0"))
	}
	t := beam.FindCombineType(col)
	funcx.MustSatisfy(less, funcx.Replace(sig, beam.TType, t))

	return beam.Combine(p, &combineFn{Less: beam.EncodedFn{Fn: reflect.ValueOf(less)}, N: n}, col)
}

// Smallest returns the smallest N elements either globally or per-key of the
// incoming PCollection, depending on its type:
//
//    PCollection<T>        : globally
//    PCollection<KV<K,T>>  : per-key
//    PCollection<GBK<K,T>> : per-key
//
// The order is defined by the comparator, less : T x T -> bool. It returns
// a single-element PCollection<[]T> or PCollection<K,[]T> with a slice of the
// N smallest elements either globally or per key.
//
// Example use:
//
//    col := beam.Create(p, 1, 11, 7, 5, 10)
//    bottom2 := stats.Smallest(p, col, 2, less)  // PCollection<[]int> with [1, 5] as the only element.
//
func Smallest(p *beam.Pipeline, col beam.PCollection, n int, less interface{}) beam.PCollection {
	p = p.Composite(fmt.Sprintf("top.Smallest(%v)", n))

	if n < 1 {
		panic(fmt.Sprintf("n must be > 0"))
	}
	t := beam.FindCombineType(col)
	funcx.MustSatisfy(less, funcx.Replace(sig, beam.TType, t))

	return beam.Combine(p, &combineFn{Less: beam.EncodedFn{Fn: reflect.ValueOf(less)}, N: n, Reversed: true}, col)
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
// Less ordering on A. The natural order maintains the largest elements.
type combineFn struct {
	// Less is the < order on the underlying type, A.
	Less beam.EncodedFn `json:"less"`
	// Reversed indicates whether the ordering should be reversed.
	Reversed bool `json:"reversed"`
	// N is the number of elements to keep.
	N int `json:"n"`
}

// TODO(herohde) 5/25/2017: a Setup/Init method would be useful.

func (f *combineFn) CreateAccumulator() accum {
	return accum{}
}

func (f *combineFn) AddInput(a accum, val beam.T) accum {
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

func (f *combineFn) ExtractOutput(a accum) []beam.T {
	var ret []beam.T
	for _, elm := range a.list {
		ret = append(ret, elm.Interface()) // implicitly wrap T
	}
	return ret
}

func (f *combineFn) trim(ret []reflect.Value) accum {
	if f.Reversed {
		sort.SliceStable(ret, func(i, j int) bool {
			return f.Less.Fn.Call([]reflect.Value{ret[i], ret[j]})[0].Bool() // uses <
		})
	} else {
		sort.SliceStable(ret, func(i, j int) bool {
			return f.Less.Fn.Call([]reflect.Value{ret[j], ret[i]})[0].Bool() // uses >
		})
	}
	if len(ret) > f.N {
		ret = ret[:f.N]
	}
	return accum{list: ret}
}
