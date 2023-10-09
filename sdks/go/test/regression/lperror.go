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

package regression

import (
	"context"
	"fmt"
	"reflect"
	"sort"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.Function2x2(toFoo)
	register.Iter1[*fruit]()
	register.Function3x1(toID)
}

// REPRO found by https://github.com/zelliott

type fruit struct {
	Name string
}

func toFoo(id int, _ func(**fruit) bool) (int, string) {
	return id, "Foo"
}

func toID(id int, fruitIter func(**fruit) bool, _ func(*string) bool) int {
	var fruit *fruit
	for fruitIter(&fruit) {
	}
	return id
}

// LPErrorPipeline constructs a pipeline that has a GBK followed by a CoGBK using the same
// input, with schema encoded structs as elements. This ends up having the stage after the
// CoGBK fail since the decoder post-cogbk is missing a Length Prefix coder that was
// applied to the GBK input, but not the CoGBK output.
// Root is likely in that there's no Beam standard CoGBK format for inject and expand.
// JIRA: BEAM-12438
func LPErrorPipeline(s beam.Scope) beam.PCollection {
	// ["Apple", "Banana", "Cherry"]
	fruits := beam.CreateList(s, []*fruit{{"Apple"}, {"Banana"}, {"Cherry"}})

	// [0 "Apple", 0 "Banana", 0 "Cherry"]
	fruitsKV := beam.AddFixedKey(s, fruits)

	// [0 ["Apple", "Banana", "Cherry"]]
	fruitsGBK := beam.GroupByKey(s, fruitsKV)

	// [0 "Foo"]
	fooKV := beam.ParDo(s, toFoo, fruitsGBK)

	// [0 ["Apple", "Banana", "Cherry"] ["Foo"]]
	fruitsFooCoGBK := beam.CoGroupByKey(s, fruitsKV, fooKV)

	// [0]
	return beam.ParDo(s, toID, fruitsFooCoGBK)
}

const (
	// MetricNamespace is the namespace for regression test metrics.
	MetricNamespace = string("regression")
	// FruitCounterName is the name of the fruit counter metric.
	FruitCounterName = string("fruitCount")
)

func sendFruit(_ []byte, emit func(fruit)) {
	emit(fruit{"Apple"})
	emit(fruit{"Banana"})
	emit(fruit{"Cherry"})
}

// countFruit counts the fruit that pass through.
func countFruit(ctx context.Context, v fruit) fruit {
	beam.NewCounter(MetricNamespace, FruitCounterName).Inc(ctx, 1)
	return v
}

type iterSideStrings struct {
	Wants []string
}

func (fn *iterSideStrings) ProcessElement(_ []byte, iter func(*fruit) bool) error {
	var val fruit
	var gots []string
	for iter(&val) {
		gots = append(gots, val.Name)
	}
	sort.Strings(gots)
	sort.Strings(fn.Wants)

	if got, want := len(gots), len(fn.Wants); got != want {
		return fmt.Errorf("len mismatch between lists. got %v, want %v; \n\t got: %v \n\twant: %v", got, want, gots, fn.Wants)
	}

	for i := range fn.Wants {
		if got, want := gots[i], fn.Wants[i]; got != want {
			return fmt.Errorf("mismatch value in sorted list at index %d: got %v, want %v", i, got, want)
		}
	}
	return nil
}

func init() {
	beam.RegisterFunction(countFruit)
	beam.RegisterFunction(sendFruit)
	beam.RegisterType(reflect.TypeOf((*iterSideStrings)(nil)))
	beam.RegisterType(reflect.TypeOf((*fruit)(nil)).Elem())
}

// LPErrorReshufflePipeline checks a Row type with reshuffle transforms.
// It's intentionally just a prefix with validation done in the specific
// test cases, as the success/failure is dependent on subsequent pipeline
// use of data.
//
// This pipeline will output a pcollection containing 3 fruit.
func LPErrorReshufflePipeline(s beam.Scope) beam.PCollection {
	sf := s.Scope("Basket")
	fruits := beam.ParDo(sf, sendFruit, beam.Impulse(sf))
	return beam.Reshuffle(sf, fruits)
}
