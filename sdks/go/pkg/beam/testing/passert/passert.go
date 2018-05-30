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

// Package passert contains verification transformations for testing pipelines.
// The transformations are not tied to any particular runner, i.e., they can
// notably be used for remote execution runners, such as Dataflow.
package passert

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/filter"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*diffFn)(nil)))
	beam.RegisterType(reflect.TypeOf((*failFn)(nil)))
	beam.RegisterType(reflect.TypeOf((*failKVFn)(nil)))
	beam.RegisterType(reflect.TypeOf((*failGBKFn)(nil)))
}

// Equals verifies the given collection has the same values as the given
// values, under coder equality. The values can be provided as single
// PCollection.
func Equals(s beam.Scope, col beam.PCollection, values ...interface{}) beam.PCollection {
	if len(values) == 0 {
		return Empty(s, col)
	}
	if other, ok := values[0].(beam.PCollection); ok && len(values) == 1 {
		return equals(s, col, other)
	}

	other := beam.Create(s, values...)
	return equals(s, col, other)
}

// equals verifies that the actual values match the expected ones.
func equals(s beam.Scope, actual, expected beam.PCollection) beam.PCollection {
	bad, _, bad2 := Diff(s, actual, expected)
	fail(s, bad, "value %v present, but not expected")
	fail(s, bad2, "value %v expected, but not present")
	return actual
}

// Diff splits 2 incoming PCollections into 3: left only, both, right only. Duplicates are
// preserved, so a value may appear multiple times and in multiple collections. Coder
// equality is used to determine equality. Should only be used for small collections,
// because all values are held in memory at the same time.
func Diff(s beam.Scope, a, b beam.PCollection) (left, both, right beam.PCollection) {
	imp := beam.Impulse(s)
	return beam.ParDo3(s, &diffFn{Coder: beam.EncodedCoder{Coder: a.Coder()}}, imp, beam.SideInput{Input: a}, beam.SideInput{Input: b})
}

// TODO(herohde) 7/11/2017: should there be a first-class way to obtain the coder,
// such a a specially-typed parameter?

// diffFn computes the symmetrical multi-set difference of 2 collections, under
// coder equality. The Go values returned may be any of the coder-equal ones.
type diffFn struct {
	Coder beam.EncodedCoder `json:"coder"`
}

func (f *diffFn) ProcessElement(_ []byte, ls, rs func(*beam.T) bool, left, both, right func(t beam.T)) error {
	c := beam.UnwrapCoder(f.Coder.Coder)

	indexL, err := index(c, ls)
	if err != nil {
		return err
	}
	indexR, err := index(c, rs)
	if err != nil {
		return err
	}

	for key, entry := range indexL {
		other, ok := indexR[key]
		if !ok {
			emitN(entry.value, entry.count, left)
			continue
		}

		diff := entry.count - other.count
		switch {
		case diff < 0:
			// More elements in rs.
			emitN(entry.value, entry.count, both)
			emitN(entry.value, -diff, right)

		case diff == 0:
			// Same amount of elements
			emitN(entry.value, entry.count, both)

		case diff > 0:
			// More elements in ls.
			emitN(entry.value, other.count, both)
			emitN(entry.value, diff, left)
		}
	}

	for key, entry := range indexR {
		if _, ok := indexL[key]; !ok {
			emitN(entry.value, entry.count, right)
		}
		// else already processed in indexL loop
	}
	return nil
}

func emitN(val beam.T, n int, emit func(t beam.T)) {
	for i := 0; i < n; i++ {
		emit(val)
	}
}

type indexEntry struct {
	count int
	value beam.T
}

func index(c *coder.Coder, iter func(*beam.T) bool) (map[string]indexEntry, error) {
	ret := make(map[string]indexEntry)
	enc := exec.MakeElementEncoder(c)

	var val beam.T
	for iter(&val) {
		var buf bytes.Buffer
		if err := enc.Encode(exec.FullValue{Elm: val}, &buf); err != nil {
			return nil, fmt.Errorf("value %v not encodable by %v", val, c)
		}
		encoded := buf.String()

		cur := ret[encoded]
		ret[encoded] = indexEntry{count: cur.count + 1, value: val}
	}
	return ret, nil
}

// TODO(herohde) 7/11/2017: perhaps extract the coder helpers as more
// general and polished utilities for working with coders in user code.

// True asserts that all elements satisfy the given predicate.
func True(s beam.Scope, col beam.PCollection, fn interface{}) beam.PCollection {
	fail(s, filter.Exclude(s, col, fn), "predicate(%v) = false, want true")
	return col
}

// False asserts that the given predicate does not satisfy any element in the condition.
func False(s beam.Scope, col beam.PCollection, fn interface{}) beam.PCollection {
	fail(s, filter.Include(s, col, fn), "predicate(%v) = true, want false")
	return col
}

// Empty asserts that col is empty.
func Empty(s beam.Scope, col beam.PCollection) beam.PCollection {
	fail(s, col, "PCollection contains %v, want empty collection")
	return col
}

// TODO(herohde) 1/24/2018: use DynFn for a unified signature here instead.

func fail(s beam.Scope, col beam.PCollection, format string) {
	switch {
	case typex.IsKV(col.Type()):
		beam.ParDo0(s, &failKVFn{Format: format}, col)

	case typex.IsCoGBK(col.Type()):
		beam.ParDo0(s, &failGBKFn{Format: format}, col)

	default:
		beam.ParDo0(s, &failFn{Format: format}, col)
	}
}

type failFn struct {
	Format string `json:"format"`
}

func (f *failFn) ProcessElement(x beam.X) error {
	return fmt.Errorf(f.Format, x)
}

type failKVFn struct {
	Format string `json:"format"`
}

func (f *failKVFn) ProcessElement(x beam.X, y beam.Y) error {
	return fmt.Errorf(f.Format, fmt.Sprintf("(%v,%v)", x, y))
}

type failGBKFn struct {
	Format string `json:"format"`
}

func (f *failGBKFn) ProcessElement(x beam.X, _ func(*beam.Y) bool) error {
	return fmt.Errorf(f.Format, fmt.Sprintf("(%v,*)", x))
}
