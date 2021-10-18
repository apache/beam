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

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
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

// applyPartition returns the 40th percentile of students.
func applyPartition(s beam.Scope, students beam.PCollection) beam.PCollection {
	// [START model_multiple_pcollections_partition]
	// Partition returns a slice of PCollections
	studentsByPercentile := beam.Partition(s, 10, decileFn, students)
	// Each partition can be extracted by indexing into the slice.
	fortiethPercentile := studentsByPercentile[4]
	// [END model_multiple_pcollections_partition]
	return fortiethPercentile
}

// [START model_pardo_side_input_dofn]

// filterWordsAbove is a DoFn that takes in a word,
// and a singleton side input iterator as of a length cut off
// and only emits words that are beneath that cut off.
//
// If the iterator has no elements, an error is returned, aborting processing.
func filterWordsAbove(word string, lengthCutOffIter func(*float64) bool, emitAboveCutoff func(string)) error {
	var cutOff float64
	ok := lengthCutOffIter(&cutOff)
	if !ok {
		return fmt.Errorf("no length cutoff provided")
	}
	if float64(len(word)) > cutOff {
		emitAboveCutoff(word)
	}
	return nil
}

// filterWordsBelow is a DoFn that takes in a word,
// and a singleton side input of a length cut off
// and only emits words that are beneath that cut off.
//
// If the side input isn't a singleton, a runtime panic will occur.
func filterWordsBelow(word string, lengthCutOff float64, emitBelowCutoff func(string)) {
	if float64(len(word)) <= lengthCutOff {
		emitBelowCutoff(word)
	}
}

func init() {
	beam.RegisterFunction(filterWordsAbove)
	beam.RegisterFunction(filterWordsBelow)
}

// [END model_pardo_side_input_dofn]

// addSideInput demonstrates passing a side input to a DoFn.
func addSideInput(s beam.Scope, words beam.PCollection) (beam.PCollection, beam.PCollection) {
	wordLengths := applyWordLen(s, words)

	// [START model_pardo_side_input]
	// avgWordLength is a PCollection containing a single element, a singleton.
	avgWordLength := stats.Mean(s, wordLengths)

	// Side inputs are added as with the beam.SideInput option to beam.ParDo.
	wordsAboveCutOff := beam.ParDo(s, filterWordsAbove, words, beam.SideInput{Input: avgWordLength})
	wordsBelowCutOff := beam.ParDo(s, filterWordsBelow, words, beam.SideInput{Input: avgWordLength})
	// [END model_pardo_side_input]
	return wordsAboveCutOff, wordsBelowCutOff
}

// isMarkedWord is a dummy function.
func isMarkedWord(word string) bool {
	return strings.HasPrefix(word, "MARKER")
}

// [START model_multiple_output_dofn]

// processWords is a DoFn that has 3 output PCollections. The emitter functions
// are matched in positional order to the PCollections returned by beam.ParDo3.
func processWords(word string, emitBelowCutoff, emitAboveCutoff, emitMarked func(string)) {
	const cutOff = 5
	if len(word) < cutOff {
		emitBelowCutoff(word)
	} else {
		emitAboveCutoff(word)
	}
	if isMarkedWord(word) {
		emitMarked(word)
	}
}

// processWordsMixed demonstrates mixing an emitter, with a standard return.
// If a standard return is used, it will always be the first returned PCollection,
// followed in positional order by the emitter functions.
func processWordsMixed(word string, emitMarked func(string)) int {
	if isMarkedWord(word) {
		emitMarked(word)
	}
	return len(word)
}

func init() {
	beam.RegisterFunction(processWords)
	beam.RegisterFunction(processWordsMixed)
}

// [END model_multiple_output_dofn]

func applyMultipleOut(s beam.Scope, words beam.PCollection) (belows, aboves, markeds, lengths, mixedMarkeds beam.PCollection) {
	// [START model_multiple_output]
	// beam.ParDo3 returns PCollections in the same order as
	// the emit function parameters in processWords.
	below, above, marked := beam.ParDo3(s, processWords, words)

	// processWordsMixed uses both a standard return and an emitter function.
	// The standard return produces the first PCollection from beam.ParDo2,
	// and the emitter produces the second PCollection.
	length, mixedMarked := beam.ParDo2(s, processWordsMixed, words)
	// [END model_multiple_output]
	return below, above, marked, length, mixedMarked
}

func extractWordsFn(line string, emitWords func(string)) {
	words := strings.Split(line, " ")
	for _, w := range words {
		emitWords(w)
	}
}

func init() {
	beam.RegisterFunction(extractWordsFn)
}

// [START countwords_composite]
// CountWords is a function that builds a composite PTransform
// to count the number of times each word appears.
func CountWords(s beam.Scope, lines beam.PCollection) beam.PCollection {
	// A subscope is required for a function to become a composite transform.
	// We assign it to the original scope variable s to shadow the original
	// for the rest of the CountWords function.
	s = s.Scope("CountWords")

	// Since the same subscope is used for the following transforms,
	// they are in the same composite PTransform.

	// Convert lines of text into individual words.
	words := beam.ParDo(s, extractWordsFn, lines)

	// Count the number of times each word occurs.
	wordCounts := stats.Count(s, words)

	// Return any PCollections that should be available after
	// the composite transform.
	return wordCounts
}

// [END countwords_composite]
