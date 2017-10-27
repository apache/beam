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

package stats

import (
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

//go:generate specialize --input=sum_switch.tmpl --x=integers,floats

// Sum returns the sum of the elements -- per key, if keyed -- in a collection.
// It expects a PCollection<A> or PCollection<KV<A,B>> as input and returns
// a singleton PCollection<A> or a PCollection<KV<A,B>>, respectively. It
// can only be used for numbers, such as int, uint16, float32, etc.
//
// For example:
//
//    col := beam.Create(p, 1, 11, 7, 5, 10)
//    sum := stats.Sum(p, col)   // PCollection<int> with 34 as the only element.
//
func Sum(p *beam.Pipeline, col beam.PCollection) beam.PCollection {
	p = p.Scope("stats.Sum")

	t := beam.FindCombineType(col)
	if !reflectx.IsNumber(t) || reflectx.IsComplex(t) {
		panic(fmt.Sprintf("Sum requires a non-complex number: %v", t))
	}

	// Do a pipeline-construction-time type switch to select the right
	// runtime operation.
	return sumSwitch(p, t, col)
}
