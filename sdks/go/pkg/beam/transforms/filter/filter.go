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
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
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
//    words := beam.Create(s, "a", "b", "long", "alsolong")
//    short := filter.Include(s, words, func(s string) bool {
//        return len(s) < 3
//    })
//
// Here, "short" will contain "a" and "b" at runtime.
func Include(s beam.Scope, col beam.PCollection, fn interface{}) beam.PCollection {
	s = s.Scope("filter.Include")

	funcx.MustSatisfy(fn, funcx.Replace(sig, beam.TType, col.Type().Type()))
	return beam.ParDo(s, &filterFn{Predicate: beam.EncodedFunc{Fn: reflectx.MakeFunc(fn)}, Include: true}, col)
}

// Exclude filters the elements of a PCollection<A> based on the given function,
// which must be of the form: A -> bool. Exclude removes all element for which
// the filter function returns true. It returns a PCollection of the same type
// as the input. For example:
//
//    words := beam.Create(s, "a", "b", "long", "alsolong")
//    long := filter.Exclude(s, words, func(s string) bool {
//        return len(s) < 3
//    })
//
// Here, "long" will contain "long" and "alsolong" at runtime.
func Exclude(s beam.Scope, col beam.PCollection, fn interface{}) beam.PCollection {
	s = s.Scope("filter.Exclude")

	funcx.MustSatisfy(fn, funcx.Replace(sig, beam.TType, col.Type().Type()))
	return beam.ParDo(s, &filterFn{Predicate: beam.EncodedFunc{Fn: reflectx.MakeFunc(fn)}, Include: false}, col)
}

type filterFn struct {
	// Predicate is the encoded predicate.
	Predicate beam.EncodedFunc `json:"predicate"`
	// Include indicates whether to include or exclude elements that satisfy the predicate.
	Include bool `json:"include"`

	fn reflectx.Func1x1
}

func (f *filterFn) Setup() {
	f.fn = reflectx.ToFunc1x1(f.Predicate.Fn)
}

func (f *filterFn) ProcessElement(elm beam.T, emit func(beam.T)) {
	match := f.fn.Call1x1(elm).(bool)
	if match == f.Include {
		emit(elm)
	}
}
