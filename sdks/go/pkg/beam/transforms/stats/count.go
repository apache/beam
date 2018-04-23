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

func init() {
	beam.RegisterFunction(mapFn)
}

// Count counts the number of elements in a collection. It expects a
// PCollection<T> as input and returns a PCollection<KV<T,int>>. T's encoding
// must be a well-defined injection.
func Count(s beam.Scope, col beam.PCollection) beam.PCollection {
	s = s.Scope("stats.Count")

	pre := beam.ParDo(s, mapFn, col)
	return SumPerKey(s, pre)
}

func mapFn(elm beam.T) (beam.T, int) {
	return elm, 1
}
