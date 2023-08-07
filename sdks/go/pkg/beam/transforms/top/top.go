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
	"fmt"
	"reflect"
	"sort"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.Combiner3[accum, beam.T, []beam.T]((*combineFn)(nil))
}

var (
	sig = funcx.MakePredicate(beam.TType, beam.TType) // (T, T) -> bool
)

// Largest returns the largest N elements of a PCollection<T>. The order is
// defined by the comparator, less : T x T -> bool. It returns a single-element
// PCollection<[]T> with a slice of the N largest elements.
//
// Example use:
//
//	col := beam.Create(s, 1, 11, 7, 5, 10)
//	top2 := stats.Largest(s, col, 2, less)  // PCollection<[]int> with [11, 10] as the only element.
func Largest(s beam.Scope, col beam.PCollection, n int, less any) beam.PCollection {
	s = s.Scope(fmt.Sprintf("top.Largest(%v)", n))

	t := beam.ValidateNonCompositeType(col)
	validate(t, n, less)

	return beam.Combine(s, newCombineFn(less, n, t.Type(), false), col)
}

// LargestPerKey returns the largest N values for each key of a PCollection<KV<K,T>>.
// The order is defined by the comparator, less : T x T -> bool. It returns a
// PCollection<KV<K,[]T>> with a slice of the N largest elements for each key.
func LargestPerKey(s beam.Scope, col beam.PCollection, n int, less any) beam.PCollection {
	s = s.Scope(fmt.Sprintf("top.LargestPerKey(%v)", n))

	_, t := beam.ValidateKVType(col)
	validate(t, n, less)

	return beam.CombinePerKey(s, newCombineFn(less, n, t.Type(), false), col)
}

// Smallest returns the smallest N elements of a PCollection<T>. The order is
// defined by the comparator, less : T x T -> bool. It returns a single-element
// PCollection<[]T> with a slice of the N smallest elements.
//
// Example use:
//
//	col := beam.Create(s, 1, 11, 7, 5, 10)
//	bottom2 := stats.Smallest(s, col, 2, less)  // PCollection<[]int> with [1, 5] as the only element.
func Smallest(s beam.Scope, col beam.PCollection, n int, less any) beam.PCollection {
	s = s.Scope(fmt.Sprintf("top.Smallest(%v)", n))

	t := beam.ValidateNonCompositeType(col)
	validate(t, n, less)

	return beam.Combine(s, newCombineFn(less, n, t.Type(), true), col)
}

// SmallestPerKey returns the smallest N values for each key of a PCollection<KV<K,T>>.
// The order is defined by the comparator, less : T x T -> bool. It returns a
// PCollection<KV<K,[]T>> with a slice of the N smallest elements for each key.
func SmallestPerKey(s beam.Scope, col beam.PCollection, n int, less any) beam.PCollection {
	s = s.Scope(fmt.Sprintf("top.SmallestPerKey(%v)", n))

	_, t := beam.ValidateKVType(col)
	validate(t, n, less)

	return beam.CombinePerKey(s, newCombineFn(less, n, t.Type(), true), col)
}

func validate(t typex.FullType, n int, less any) {
	if n < 1 {
		panic("n must be > 0")
	}
	funcx.MustSatisfy(less, funcx.Replace(sig, beam.TType, t.Type()))
}

func newCombineFn(less any, n int, t reflect.Type, reversed bool) *combineFn {
	fn := &combineFn{Less: beam.EncodedFunc{Fn: reflectx.MakeFunc(less)}, N: n, Type: beam.EncodedType{T: t}, Reversed: reversed}
	// Running SetupFn at pipeline construction helps validate the
	// combineFn, and simplify testing.
	fn.Setup()
	return fn
}

// TODO(herohde) 5/25/2017: use a heap instead of a sorted slice.

type accum struct {
	enc beam.ElementEncoder
	dec beam.ElementDecoder

	data [][]byte
	// list stores the elements of type A in order. It has at most size N.
	list []any
}

func (a *accum) unmarshal() error {
	if a.data == nil {
		return nil
	}
	for _, val := range a.data {
		element, err := a.dec.Decode(bytes.NewBuffer(val))
		if err != nil {
			return errors.WithContextf(err, "top.accum: unmarshalling")
		}
		a.list = append(a.list, element)
	}
	a.data = nil
	return nil
}

var (
	accumType = reflect.TypeOf((*accum)(nil)).Elem()
)

func init() {
	beam.RegisterType(accumType)
	beam.RegisterCoder(accumType, accumEnc(), accumDec())
}

func accumEnc() func(accum) ([]byte, error) {
	byteEnc, err := coder.EncoderForSlice(reflect.TypeOf((*[][]byte)(nil)).Elem())
	if err != nil {
		panic(err)
	}
	return func(a accum) ([]byte, error) {
		if len(a.list) > 0 && a.enc == nil {
			return nil, errors.Errorf("top.accum: element encoder unspecified with non-zero elements: %v data available", len(a.data))
		}
		var values [][]byte
		if len(a.list) == 0 && len(a.data) > 0 {
			values = a.data
		}
		for _, value := range a.list {
			var buf bytes.Buffer
			if err := a.enc.Encode(value, &buf); err != nil {
				return nil, errors.WithContextf(err, "top.accum: marshalling %v", value)
			}
			values = append(values, buf.Bytes())
		}

		var buf bytes.Buffer
		if err := coder.WriteSimpleRowHeader(1, &buf); err != nil {
			return nil, err
		}
		if err := byteEnc(values, &buf); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
}

func accumDec() func([]byte) (accum, error) {
	byteDec, err := coder.DecoderForSlice(reflect.TypeOf((*[][]byte)(nil)).Elem())
	if err != nil {
		panic(err)
	}
	return func(b []byte) (accum, error) {
		buf := bytes.NewBuffer(b)
		if err := coder.ReadSimpleRowHeader(1, buf); err != nil {
			return accum{}, err
		}
		s, err := byteDec(buf)
		if err != nil {
			return accum{}, err
		}
		return accum{
			data: s.([][]byte),
		}, nil
	}
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
	// Type is the element type A
	Type beam.EncodedType `json:"type"`

	enc  beam.ElementEncoder
	dec  beam.ElementDecoder
	less reflectx.Func2x1
}

func (f *combineFn) Setup() {
	f.enc = beam.NewElementEncoder(f.Type.T)
	f.dec = beam.NewElementDecoder(f.Type.T)
}

func (f *combineFn) CreateAccumulator() accum {
	return accum{enc: f.enc, dec: f.dec}
}

func (f *combineFn) AddInput(a accum, val beam.T) accum {
	ret := append(a.list, val)
	return f.trim(ret)
}

func (f *combineFn) MergeAccumulators(a, b accum) accum {
	a.enc, a.dec = f.enc, f.dec
	b.enc, b.dec = f.enc, f.dec
	if err := a.unmarshal(); err != nil {
		panic(err)
	}
	if err := b.unmarshal(); err != nil {
		panic(err)
	}
	ret := append(a.list, b.list...)
	return f.trim(ret)
}

func (f *combineFn) ExtractOutput(a accum) []beam.T {
	var ret []beam.T
	a.enc, a.dec = f.enc, f.dec
	if err := a.unmarshal(); err != nil {
		panic(err)
	}
	for _, elm := range a.list {
		ret = append(ret, elm) // implicitly wrap T
	}
	return ret
}

func (f *combineFn) trim(ret []any) accum {
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
	return accum{enc: f.enc, dec: f.dec, list: ret}
}
