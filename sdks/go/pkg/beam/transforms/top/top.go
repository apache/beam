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
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

//go:generate go install github.com/apache/beam/sdks/go/cmd/starcgen
//go:generate starcgen --package=top --identifiers=combineFn

var (
	sig = funcx.MakePredicate(beam.TType, beam.TType) // (T, T) -> bool
)

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

	return beam.Combine(s, newCombineFn(less, n, t, false), col)
}

// LargestPerKey returns the largest N values for each key of a PCollection<KV<K,T>>.
// The order is defined by the comparator, less : T x T -> bool. It returns a
// single-element PCollection<KV<K,[]T>> with a slice of the N largest elements for
// each key.
func LargestPerKey(s beam.Scope, col beam.PCollection, n int, less interface{}) beam.PCollection {
	s = s.Scope(fmt.Sprintf("top.LargestPerKey(%v)", n))

	_, t := beam.ValidateKVType(col)
	validate(t, n, less)

	return beam.CombinePerKey(s, newCombineFn(less, n, t, false), col)
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

	return beam.Combine(s, newCombineFn(less, n, t, true), col)
}

// SmallestPerKey returns the smallest N values for each key of a PCollection<KV<K,T>>.
// The order is defined by the comparator, less : T x T -> bool. It returns a
// single-element PCollection<KV<K,[]T>> with a slice of the N smallest elements for
// each key.
func SmallestPerKey(s beam.Scope, col beam.PCollection, n int, less interface{}) beam.PCollection {
	s = s.Scope(fmt.Sprintf("top.SmallestPerKey(%v)", n))

	_, t := beam.ValidateKVType(col)
	validate(t, n, less)

	return beam.Combine(s, newCombineFn(less, n, t, true), col)
}

func validate(t typex.FullType, n int, less interface{}) {
	if n < 1 {
		panic(fmt.Sprintf("n must be > 0"))
	}
	funcx.MustSatisfy(less, funcx.Replace(sig, beam.TType, t.Type()))
}

func newCombineFn(less interface{}, n int, t typex.FullType, reversed bool) *combineFn {
	coder := beam.NewCoder(t)
	return &combineFn{Less: beam.EncodedFunc{Fn: reflectx.MakeFunc(less)}, N: n, Coder: beam.EncodedCoder{Coder: coder}, Reversed: reversed}
}

// TODO(herohde) 5/25/2017: use a heap instead of a sorted slice.

type accum struct {
	coder beam.Coder
	data  [][]byte
	// list stores the elements of type A in order. It has at most size N.
	list []interface{}
}

// UnmarshalJSON allows accum to hook into the JSON Decoder, and
// deserialize it's own representation.
func (a *accum) UnmarshalJSON(b []byte) error {
	json.Unmarshal(b, &a.data)
	return nil
}

func (a *accum) unmarshal() error {
	if a.data == nil {
		return nil
	}
	dec := exec.MakeElementDecoder(beam.UnwrapCoder(a.coder))
	for _, val := range a.data {
		fv, err := dec.Decode(bytes.NewBuffer(val))
		if err != nil {
			return fmt.Errorf("top.accum: error unmarshal: %v", err)
		}
		a.list = append(a.list, fv.Elm)
	}
	a.data = nil
	return nil
}

// MarshalJSON uses the hook into the JSON encoder library to
func (a accum) MarshalJSON() ([]byte, error) {
	if !a.coder.IsValid() {
		return nil, fmt.Errorf("top.accum: element coder unspecified")
	}
	enc := exec.MakeElementEncoder(beam.UnwrapCoder(a.coder))
	var values [][]byte
	for _, value := range a.list {
		var buf bytes.Buffer
		if err := enc.Encode(exec.FullValue{Elm: value}, &buf); err != nil {
			return nil, fmt.Errorf("top.accum: marshalling of %v failed: %v", value, err)
		}
		values = append(values, buf.Bytes())
	}
	a.list = nil
	return json.Marshal(values)
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
	// Coder is the element coder for the underlying type, A.
	Coder beam.EncodedCoder `json:"coder"`

	less reflectx.Func2x1
}

func (f *combineFn) CreateAccumulator() accum {
	return accum{coder: f.Coder.Coder}
}

func (f *combineFn) AddInput(a accum, val beam.T) accum {
	ret := append(a.list, val)
	return f.trim(ret)
}

func (f *combineFn) MergeAccumulators(a, b accum) accum {
	a.coder = f.Coder.Coder
	b.coder = f.Coder.Coder
	if err := a.unmarshal(); err != nil {
		panic(err)
	}
	if err := b.unmarshal(); err != nil {
		panic(err)
	}
	var ret []interface{}
	ret = append(a.list, b.list...)
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
	return accum{coder: f.Coder.Coder, list: ret}
}
