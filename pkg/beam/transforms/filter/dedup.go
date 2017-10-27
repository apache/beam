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

package filter

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
)

// Dedup removes all duplicates from a collection, under coder equality. It
// expects a PCollection<T> as input and returns a PCollection<T> with
// duplicates removed.
func Dedup(p *beam.Pipeline, col beam.PCollection) beam.PCollection {
	p = p.Scope("filter.Dedup")

	pre := beam.ParDo(p, mapFn, col)
	post := beam.GroupByKey(p, pre)
	return beam.ParDo(p, keyFn, post)
}

func mapFn(elm beam.T) (beam.T, int) {
	return elm, 1
}

func keyFn(key beam.T, _ func(*int) bool) beam.T {
	return key
}
