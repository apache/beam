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
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
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

// [START cogroupbykey_input_helpers]

type stringPair struct {
	K, V string
}

func splitStringPair(e stringPair) (string, string) {
	return e.K, e.V
}

func init() {
	// Register element types and DoFns.
	beam.RegisterType(reflect.TypeOf((*stringPair)(nil)).Elem())
	beam.RegisterFunction(splitStringPair)
}

// CreateAndSplit is a helper function that creates
func CreateAndSplit(s beam.Scope, input []stringPair) beam.PCollection {
	initial := beam.CreateList(s, input)
	return beam.ParDo(s, splitStringPair, initial)
}

// [END cogroupbykey_input_helpers]

// [START cogroupbykey_output_helpers]

func formatCoGBKResults(key string, emailIter, phoneIter func(*string) bool) string {
	var s string
	var emails, phones []string
	for emailIter(&s) {
		emails = append(emails, s)
	}
	for phoneIter(&s) {
		phones = append(phones, s)
	}
	// Values have no guaranteed order, sort for deterministic output.
	sort.Strings(emails)
	sort.Strings(phones)
	return fmt.Sprintf("%s; %s; %s", key, formatStringIter(emails), formatStringIter(phones))
}

func init() {
	beam.RegisterFunction(formatCoGBKResults)
}

// [END cogroupbykey_output_helpers]

func formatStringIter(vs []string) string {
	var b strings.Builder
	b.WriteRune('[')
	for i, v := range vs {
		b.WriteRune('\'')
		b.WriteString(v)
		b.WriteRune('\'')
		if i < len(vs)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteRune(']')
	return b.String()
}

func coGBKExample(s beam.Scope) beam.PCollection {
	// [START cogroupbykey_inputs]
	var emailSlice = []stringPair{
		{"amy", "amy@example.com"},
		{"carl", "carl@example.com"},
		{"julia", "julia@example.com"},
		{"carl", "carl@email.com"},
	}

	var phoneSlice = []stringPair{
		{"amy", "111-222-3333"},
		{"james", "222-333-4444"},
		{"amy", "333-444-5555"},
		{"carl", "444-555-6666"},
	}
	emails := CreateAndSplit(s.Scope("CreateEmails"), emailSlice)
	phones := CreateAndSplit(s.Scope("CreatePhones"), phoneSlice)
	// [END cogroupbykey_inputs]

	// [START cogroupbykey_outputs]
	results := beam.CoGroupByKey(s, emails, phones)

	contactLines := beam.ParDo(s, formatCoGBKResults, results)
	// [END cogroupbykey_outputs]

	return contactLines
}

// [START combine_simple_sum]
func sumInts(a, v int) int {
	return a + v
}

func init() {
	beam.RegisterFunction(sumInts)
}

func globallySumInts(s beam.Scope, ints beam.PCollection) beam.PCollection {
	return beam.Combine(s, sumInts, ints)
}

type boundedSum struct {
	Bound int
}

func (fn *boundedSum) MergeAccumulators(a, v int) int {
	sum := a + v
	if fn.Bound > 0 && sum > fn.Bound {
		return fn.Bound
	}
	return sum
}

func init() {
	beam.RegisterType(reflect.TypeOf((*boundedSum)(nil)))
}

func globallyBoundedSumInts(s beam.Scope, bound int, ints beam.PCollection) beam.PCollection {
	return beam.Combine(s, &boundedSum{Bound: bound}, ints)
}

// [END combine_simple_sum]

// [START combine_custom_average]

type averageFn struct{}

type averageAccum struct {
	Count, Sum int
}

func (fn *averageFn) CreateAccumulator() averageAccum {
	return averageAccum{0, 0}
}

func (fn *averageFn) AddInput(a averageAccum, v int) averageAccum {
	return averageAccum{Count: a.Count + 1, Sum: a.Sum + v}
}

func (fn *averageFn) MergeAccumulators(a, v averageAccum) averageAccum {
	return averageAccum{Count: a.Count + v.Count, Sum: a.Sum + v.Sum}
}

func (fn *averageFn) ExtractOutput(a averageAccum) float64 {
	if a.Count == 0 {
		return math.NaN()
	}
	return float64(a.Sum) / float64(a.Count)
}

func init() {
	beam.RegisterType(reflect.TypeOf((*averageFn)(nil)))
}

// [END combine_custom_average]

func globallyAverage(s beam.Scope, ints beam.PCollection) beam.PCollection {
	// [START combine_global_average]
	average := beam.Combine(s, &averageFn{}, ints)
	// [END combine_global_average]
	return average
}

func globallyAverageWithDefault(s beam.Scope, ints beam.PCollection) beam.PCollection {
	// [START combine_global_with_default]
	// Setting combine defaults has requires no helper function in the Go SDK.
	average := beam.Combine(s, &averageFn{}, ints)

	// To add a default value:
	defaultValue := beam.Create(s, float64(0))
	avgWithDefault := beam.ParDo(s, func(d float64, iter func(*float64) bool) float64 {
		var c float64
		if iter(&c) {
			// Side input has a value, so return it.
			return c
		}
		// Otherwise, return the default
		return d
	}, defaultValue, beam.SideInput{Input: average})
	// [END combine_global_with_default]
	return avgWithDefault
}

func perKeyAverage(s beam.Scope, playerAccuracies beam.PCollection) beam.PCollection {
	// [START combine_per_key]
	avgAccuracyPerPlayer := stats.MeanPerKey(s, playerAccuracies)
	// [END combine_per_key]
	return avgAccuracyPerPlayer
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

func decileFn(student Student) int {
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
