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
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

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

	// [0 ["Foo"] ["Apple", "Banana", "Cherry"]]
	fruitsFooCoGBK := beam.CoGroupByKey(s, fruitsKV, fooKV)

	// [0]
	return beam.ParDo(s, toID, fruitsFooCoGBK)
}
