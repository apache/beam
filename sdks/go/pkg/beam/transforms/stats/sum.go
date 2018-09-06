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
	"github.com/apache/beam/sdks/go/pkg/beam"
)

//go:generate specialize --input=sum_switch.tmpl --x=integers,floats
//go:generate gofmt -w sum_switch.go

// Sum returns the sum of the elements in a PCollection<A> as a singleton
// PCollection<A>. It can only be used for numbers, such as int, uint16,
// float32, etc.
//
// For example:
//
//    col := beam.Create(s, 1, 11, 7, 5, 10)
//    sum := stats.Sum(s, col)   // PCollection<int> with 34 as the only element.
//
func Sum(s beam.Scope, col beam.PCollection) beam.PCollection {
	s = s.Scope("stats.Sum")
	return combine(s, findSumFn, col)
}

// SumPerKey returns the sum of the values per key in a PCollection<KV<A,B>> as
// a PCollection<KV<A,B>>. It can only be used for value numbers, such as int,
// uint16, float32, etc.
func SumPerKey(s beam.Scope, col beam.PCollection) beam.PCollection {
	s = s.Scope("stats.SumPerKey")
	return combinePerKey(s, findSumFn, col)
}
