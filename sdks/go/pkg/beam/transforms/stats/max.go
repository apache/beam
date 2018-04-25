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

//go:generate specialize --input=max_switch.tmpl --x=integers,floats
//go:generate gofmt -w max_switch.go

// Max returns the maximal element in a PCollection<A> as a singleton
// PCollection<A>. It can only be used for numbers, such as int, uint16,
// float32, etc.
//
// For example:
//
//    col := beam.Create(s, 1, 11, 7, 5, 10)
//    max := stats.Max(s, col)   // PCollection<int> with 11 as the only element.
//
func Max(s beam.Scope, col beam.PCollection) beam.PCollection {
	s = s.Scope("stats.Max")
	return combine(s, findMaxFn, col)
}

// MaxPerKey returns the maximal element per key in a PCollection<KV<A,B>> as
// a PCollection<KV<A,B>>. It can only be used for numbers, such as int,
// uint16, float32, etc.
func MaxPerKey(s beam.Scope, col beam.PCollection) beam.PCollection {
	s = s.Scope("stats.MaxPerKey")
	return combinePerKey(s, findMaxFn, col)
}
