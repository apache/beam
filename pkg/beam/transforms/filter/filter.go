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

// Package filter contains transformations for removing pipeline elements based on
// various conditions.
package filter

import (
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

var (
	sig = funcx.MakePredicate(beam.TType) // T -> bool
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
	p = p.Scope("filter.Include")

	t := typex.SkipW(col.Type()).Type()
	funcx.MustSatisfy(fn, funcx.Replace(sig, beam.TType, t))

	return beam.ParDo(p, &filterFn{Predicate: beam.EncodedFn{Fn: reflect.ValueOf(fn)}, Include: true}, col)
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
	p = p.Scope("filter.Exclude")

	t := typex.SkipW(col.Type()).Type()
	funcx.MustSatisfy(fn, funcx.Replace(sig, beam.TType, t))

	return beam.ParDo(p, &filterFn{Predicate: beam.EncodedFn{Fn: reflect.ValueOf(fn)}, Include: false}, col)
}

type filterFn struct {
	// Predicate is the encoded predicate.
	Predicate beam.EncodedFn `json:"predicate"`
	// Include indicates whether to include or exclude elements that satisfy the predicate.
	Include bool `json:"include"`
}

func (f *filterFn) ProcessElement(elm beam.T, emit func(beam.T)) {
	ret := f.Predicate.Fn.Call([]reflect.Value{reflect.ValueOf(elm)})
	if ret[0].Bool() == f.Include {
		emit(elm)
	}
}
