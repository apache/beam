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

package snippets

import (
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
)

// [START model_pardo_pardo]

// ComputeWordLengthFn is the DoFn to perform on each element in the input PCollection.
type ComputeWordLengthFn struct{}

// ProcessElement is the method to execute for each element.
func (fn *ComputeWordLengthFn) ProcessElement(word string, emit func(int)) {
	emit(len(word))
}

// DoFns must be registered with beam.
func init() {
	beam.RegisterType(reflect.TypeOf((*ComputeWordLengthFn)(nil)))
}

// [END model_pardo_pardo]

// applyWordLen applies ComputeWordLengthFn to words, which must be
// a PCollection<string>
func applyWordLen(s beam.Scope, words beam.PCollection) beam.PCollection {
	// [START model_pardo_apply]
	wordLengths := beam.ParDo(s, &ComputeWordLengthFn{}, words)
	// [END model_pardo_apply]
	return wordLengths
}

func applyWordLenAnon(s beam.Scope, words beam.PCollection) beam.PCollection {
	// [START model_pardo_apply_anon]
	// Apply an anonymous function as a DoFn PCollection words.
	// Save the result as the PCollection wordLengths.
	wordLengths := beam.ParDo(s, func(word string) int {
		return len(word)
	}, words)
	// [END model_pardo_apply_anon]
	return wordLengths
}

func applyFlatten(s beam.Scope, pcol1, pcol2, pcol3 beam.PCollection) beam.PCollection {
	// [START model_multiple_pcollections_flatten]
	merged := beam.Flatten(s, pcol1, pcol2, pcol3)
	// [END model_multiple_pcollections_flatten]
	return merged
}

type Student struct {
	Percentile int
}

// [START model_multiple_pcollections_partition_fn]

func decileFn(student *Student) int {
	return int(float64(student.Percentile) / float64(10))
}

func init() {
	beam.RegisterFunction(decileFn)
}

// [END model_multiple_pcollections_partition_fn]

func applyPartition(s beam.Scope, students beam.PCollection) beam.PCollection {
	// [START model_multiple_pcollections_partition]
	// Partition returns a slice of PCollections
	studentsByPercentile := beam.Partition(s, 10, decileFn, students)
	// Each partition can be extracted by indexing into the slice.
	fortiethPercentile := studentsByPercentile[4]
	// [END model_multiple_pcollections_partition]
	return fortiethPercentile
}
