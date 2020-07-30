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

// Package stats contains transforms for statistical processing.
package stats

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

// Count counts the number of appearances of each element in a collection. It
// expects a PCollection<T> as input and returns a PCollection<KV<T,int>>. T's
// encoding must be deterministic so it is valid as a key.
func Count(s beam.Scope, col beam.PCollection) beam.PCollection {
	s = s.Scope("stats.Count")

	pre := beam.ParDo(s, keyedCountFn, col)
	return SumPerKey(s, pre)
}

func keyedCountFn(elm beam.T) (beam.T, int) {
	return elm, 1
}

// CountElms counts the number of elements in a collection. It expects a
// PCollection<T> as input and returns a PCollection<int> of one element
// containing the count.
func CountElms(s beam.Scope, col beam.PCollection) beam.PCollection {
	s = s.Scope("stats.CountElms")

	if typex.IsKV(col.Type()) {
		col = beam.DropKey(s, col)
	}
	pre := beam.ParDo(s, countFn, col)
	zero := beam.Create(s, 0)
	return Sum(s, beam.Flatten(s, pre, zero))
}

func countFn(_ beam.T) int {
	return 1
}
