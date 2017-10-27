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

//go:generate specialize --input=min_switch.tmpl --x=integers,floats

// Min returns the minimal element -- per key, if keyed -- in a collection.
// It expects a PCollection<A> or PCollection<KV<A,B>> as input and returns
// a singleton PCollection<A> or a PCollection<KV<A,B>>, respectively. It
// can only be used for numbers, such as int, uint16, float32, etc.
//
// For example:
//
//    col := beam.Create(p, 1, 11, 7, 5, 10)
//    min := stats.Min(p, col)   // PCollection<int> with 1 as the only element.
//
func Min(p *beam.Pipeline, col beam.PCollection) beam.PCollection {
	p = p.Scope("stats.Min")

	t := beam.FindCombineType(col)
	if !reflectx.IsNumber(t) || reflectx.IsComplex(t) {
		panic(fmt.Sprintf("Min requires a non-complex number: %v", t))
	}

	// Do a pipeline-construction-time type switch to select the right
	// runtime operation.
	return minSwitch(p, t, col)
}
