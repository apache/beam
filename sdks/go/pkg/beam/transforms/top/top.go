// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

var (
	sig = funcx.MakePredicate(beam.TType, beam.TType) // (T, T) -> bool
)

func init() {
	beam.RegisterType(reflect.TypeOf((*combineFn)(nil)).Elem())
}

// Largest returns the largest N elements of a PCollection<T>. The order is
// defined by the comparator, less : T x T -> bool. It returns a single-element
// PCollection<[]T> with a slice of the N largest elements.
//
// Example use:
//
//    col := beam.Create(s, 1, 11, 7, 5, 10)
//    top2 := stats.Largest(s, col, 2, less)  // PCollection<[]int> with [11, 10] as the only element.
//
func Largest(s beam.Scope, col beam.PCollection, n int, less interface{}) beam.PCollection {
	s = s.Scope(fmt.Sprintf("top.Largest(%v)", n))

	t := beam.ValidateNonCompositeType(col)
	validate(t, n, less)

	return beam.Combine(s, &combineFn{Less: beam.EncodedFunc{Fn: reflectx.MakeFunc(less)}, N: n}, col)
}

// LargestPerKey returns the largest N values for each key of a PCollection<KV<K,T>>.
// The order is defined by the comparator, less : T x T -> bool. It returns a
// single-element PCollection<KV<K,[]T>> with a slice of the N largest elements for
// each key.
func LargestPerKey(s beam.Scope, col beam.PCollection, n int, less interface{}) beam.PCollection {
	s = s.Scope(fmt.Sprintf("top.LargestPerKey(%v)", n))

	_, t := beam.ValidateKVType(col)
	validate(t, n, less)

	return beam.CombinePerKey(s, &combineFn{Less: beam.EncodedFunc{Fn: reflectx.MakeFunc(less)}, N: n}, col)
}

// Smallest returns the smallest N elements of a PCollection<T>. The order is
// defined by the comparator, less : T x T -> bool. It returns a single-element
// PCollection<[]T> with a slice of the N smallest elements.
//
// Example use:
//
//    col := beam.Create(s, 1, 11, 7, 5, 10)
//    bottom2 := stats.Smallest(s, col, 2, less)  // PCollection<[]int> with [1, 5] as the only element.
//
func Smallest(s beam.Scope, col beam.PCollection, n int, less interface{}) beam.PCollection {
	s = s.Scope(fmt.Sprintf("top.Smallest(%v)", n))

	t := beam.ValidateNonCompositeType(col)
	validate(t, n, less)

	return beam.Combine(s, &combineFn{Less: beam.EncodedFunc{Fn: reflectx.MakeFunc(less)}, N: n, Reversed: true}, col)
}

// SmallestPerKey returns the smallest N values for each key of a PCollection<KV<K,T>>.
// The order is defined by the comparator, less : T x T -> bool. It returns a
// single-element PCollection<KV<K,[]T>> with a slice of the N smallest elements for
// each key.
func SmallestPerKey(s beam.Scope, col beam.PCollection, n int, less interface{}) beam.PCollection {
	s = s.Scope(fmt.Sprintf("top.SmallestPerKey(%v)", n))

	_, t := beam.ValidateKVType(col)
	validate(t, n, less)

	return beam.Combine(s, &combineFn{Less: beam.EncodedFunc{Fn: reflectx.MakeFunc(less)}, N: n, Reversed: true}, col)
}

func validate(t typex.FullType, n int, less interface{}) {
	if n < 1 {
		panic(fmt.Sprintf("n must be > 0"))
	}
	funcx.MustSatisfy(less, funcx.Replace(sig, beam.TType, t.Type()))
}

// TODO(herohde) 5/25/2017: the accumulator should be serializable with a Coder.
// We need a coder here, because the elements are generally code-able only. Until
// then, we do not support combiner lifting.

// TODO(herohde) 5/25/2017: use a heap instead of a sorted slice.

type accum struct {
	// list stores the elements of type A in order. It has at most size N.
	list []interface{}
}

// combineFn is the internal CombineFn. It maintains accumulators containing
// sorted lists of element of the underlying type, A, up to size N, under the
// Less ordering on A. The natural order maintains the largest elements.
type combineFn struct {
	// Less is the < order on the underlying type, A.
	Less beam.EncodedFunc `json:"less"`
	// Reversed indicates whether the ordering should be reversed.
	Reversed bool `json:"reversed"`
	// N is the number of elements to keep.
	N int `json:"n"`

	less reflectx.Func2x1
}

// TODO(herohde) 5/25/2017: a Setup/Init method would be useful.

func (f *combineFn) CreateAccumulator() accum {
	return accum{}
}

func (f *combineFn) AddInput(a accum, val beam.T) accum {
	t := f.Less.Fn.Type().In(0)                 // == underlying type, A
	ret := append(a.list, exec.Convert(val, t)) // unwrap T
	return f.trim(ret)
}

func (f *combineFn) MergeAccumulators(list []accum) accum {
	var ret []interface{}
	for _, a := range list {
		ret = append(ret, a.list...)
	}
	return f.trim(ret)
}

func (f *combineFn) ExtractOutput(a accum) []beam.T {
	var ret []beam.T
	for _, elm := range a.list {
		ret = append(ret, elm) // implicitly wrap T
	}
	return ret
}

func (f *combineFn) trim(ret []interface{}) accum {
	if f.less == nil {
		f.less = reflectx.ToFunc2x1(f.Less.Fn)
	}

	if f.Reversed {
		sort.SliceStable(ret, func(i, j int) bool {
			return f.less.Call2x1(ret[i], ret[j]).(bool) // uses <
		})
	} else {
		sort.SliceStable(ret, func(i, j int) bool {
			return f.less.Call2x1(ret[j], ret[i]).(bool) // uses >
		})
	}
	if len(ret) > f.N {
		ret = ret[:f.N]
	}
	return accum{list: ret}
}
