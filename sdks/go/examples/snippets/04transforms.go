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
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/timers"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
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
	// 2 inputs and 0 outputs => DoFn2x0
	// 1 input => Emitter1
	// Input/output types are included in order in the brackets
	register.DoFn2x0[string, func(int)](&ComputeWordLengthFn{})
	register.Emitter1[int]()
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

// [START model_pardo_apply_anon]

func wordLengths(word string) int { return len(word) }
func init()                       { register.Function1x1(wordLengths) }

func applyWordLenAnon(s beam.Scope, words beam.PCollection) beam.PCollection {
	return beam.ParDo(s, wordLengths, words)
}

// [END model_pardo_apply_anon]

func applyGbk(s beam.Scope, input []stringPair) beam.PCollection {
	// [START groupbykey]
	// CreateAndSplit creates and returns a PCollection with <K,V>
	// from an input slice of stringPair (struct with K, V string fields).
	pairs := CreateAndSplit(s, input)
	keyed := beam.GroupByKey(s, pairs)
	// [END groupbykey]
	return keyed
}

// [START cogroupbykey_input_helpers]

type stringPair struct {
	K, V string
}

func splitStringPair(e stringPair) (string, string) {
	return e.K, e.V
}

func init() {
	// Register DoFn.
	register.Function1x2(splitStringPair)
}

// CreateAndSplit is a helper function that creates
func CreateAndSplit(s beam.Scope, input []stringPair) beam.PCollection {
	initial := beam.CreateList(s, input)
	return beam.ParDo(s, splitStringPair, initial)
}

// [END cogroupbykey_input_helpers]

type splittableDoFn struct{}

type weDoFn struct{}

// [START bundlefinalization_simplecallback]

func (fn *splittableDoFn) ProcessElement(bf beam.BundleFinalization, rt *sdf.LockRTracker, element string) {
	// ... produce output ...

	bf.RegisterCallback(5*time.Minute, func() error {
		// ... perform a side effect ...

		return nil
	})
}

// [END bundlefinalization_simplecallback]

// [START watermarkestimation_customestimator]

// WatermarkState is a custom type.`
//
// It is optional to write your own state type when making a custom estimator.
type WatermarkState struct {
	Watermark time.Time
}

// CustomWatermarkEstimator is a custom watermark estimator.
// You may use any type here, including some of Beam's built in watermark estimator types,
// e.g. sdf.WallTimeWatermarkEstimator, sdf.TimestampObservingWatermarkEstimator, and sdf.ManualWatermarkEstimator
type CustomWatermarkEstimator struct {
	state WatermarkState
}

// CurrentWatermark returns the current watermark and is invoked on DoFn splits and self-checkpoints.
// Watermark estimators must implement CurrentWatermark() time.Time
func (e *CustomWatermarkEstimator) CurrentWatermark() time.Time {
	return e.state.Watermark
}

// ObserveTimestamp is called on the output timestamps of all
// emitted elements to update the watermark. It is optional
func (e *CustomWatermarkEstimator) ObserveTimestamp(ts time.Time) {
	e.state.Watermark = ts
}

// InitialWatermarkEstimatorState defines an initial state used to initialize the watermark
// estimator. It is optional. If this is not defined, WatermarkEstimatorState may not be
// defined and CreateWatermarkEstimator must not take in parameters.
func (fn *weDoFn) InitialWatermarkEstimatorState(et beam.EventTime, rest offsetrange.Restriction, element string) WatermarkState {
	// Return some watermark state
	return WatermarkState{Watermark: time.Now()}
}

// CreateWatermarkEstimator creates the watermark estimator used by this Splittable DoFn.
// Must take in a state parameter if InitialWatermarkEstimatorState is defined, otherwise takes no parameters.
func (fn *weDoFn) CreateWatermarkEstimator(initialState WatermarkState) *CustomWatermarkEstimator {
	return &CustomWatermarkEstimator{state: initialState}
}

// WatermarkEstimatorState returns the state used to resume future watermark estimation
// after a checkpoint/split. It is required if InitialWatermarkEstimatorState is defined,
// otherwise it must not be defined.
func (fn *weDoFn) WatermarkEstimatorState(e *CustomWatermarkEstimator) WatermarkState {
	return e.state
}

// ProcessElement is the method to execute for each element.
// It can optionally take in a watermark estimator.
func (fn *weDoFn) ProcessElement(e *CustomWatermarkEstimator, element string) {
	// ...
	e.state.Watermark = time.Now()
}

// [END watermarkestimation_customestimator]

// [START sdf_truncate]

// TruncateRestriction is a transform that is triggered when pipeline starts to drain. It helps to finish a
// pipeline quicker by truncating the restriction.
func (fn *splittableDoFn) TruncateRestriction(rt *sdf.LockRTracker, element string) offsetrange.Restriction {
	start := rt.GetRestriction().(offsetrange.Restriction).Start
	prevEnd := rt.GetRestriction().(offsetrange.Restriction).End
	// truncate the restriction by half.
	newEnd := prevEnd / 2
	return offsetrange.Restriction{
		Start: start,
		End:   newEnd,
	}
}

// [END sdf_truncate]

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
	register.Function3x1(formatCoGBKResults)
	// 1 input of type string => Iter1[string]
	register.Iter1[string]()
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
	register.Function2x1(sumInts)
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
	register.Combiner1[int](&boundedSum{})
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

func (fn *averageFn) Compact(a averageAccum) averageAccum {
	// No-op
	return a
}

func init() {
	register.Combiner3[averageAccum, int, float64](&averageFn{})
}

// [END combine_custom_average]

func globallyAverage(s beam.Scope, ints beam.PCollection) beam.PCollection {
	// [START combine_global_average]
	average := beam.Combine(s, &averageFn{}, ints)
	// [END combine_global_average]
	return average
}

// [START combine_global_with_default]

func returnSideOrDefault(d float64, iter func(*float64) bool) float64 {
	var c float64
	if iter(&c) {
		// Side input has a value, so return it.
		return c
	}
	// Otherwise, return the default
	return d
}
func init() { register.Function2x1(returnSideOrDefault) }

func globallyAverageWithDefault(s beam.Scope, ints beam.PCollection) beam.PCollection {
	// Setting combine defaults has requires no helper function in the Go SDK.
	average := beam.Combine(s, &averageFn{}, ints)

	// To add a default value:
	defaultValue := beam.Create(s, float64(0))
	return beam.ParDo(s, returnSideOrDefault, defaultValue, beam.SideInput{Input: average})
}

// [END combine_global_with_default]
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
	register.Function1x1(decileFn)
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
	register.Function3x1(filterWordsAbove)
	register.Function3x0(filterWordsBelow)
	// 1 input of type string => Emitter1[string]
	register.Emitter1[string]()
	// 1 input of type float64 => Iter1[float64]
	register.Iter1[float64]()
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
	register.Function4x0(processWords)
	register.Function2x1(processWordsMixed)
	// 1 input of type string => Emitter1[string]
	register.Emitter1[string]()
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

// [START model_paneinfo]

func extractWordsFn(pn beam.PaneInfo, line string, emitWords func(string)) {
	if pn.Timing == typex.PaneEarly || pn.Timing == typex.PaneOnTime {
		// ... perform operation ...
	}
	if pn.Timing == typex.PaneLate {
		// ... perform operation ...
	}
	if pn.IsFirst {
		// ... perform operation ...
	}
	if pn.IsLast {
		// ... perform operation ...
	}

	words := strings.Split(line, " ")
	for _, w := range words {
		emitWords(w)
	}
}

// [END model_paneinfo]

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// [START state_and_timers]

// stateAndTimersFn is an example stateful DoFn with state and a timer.
type stateAndTimersFn struct {
	Buffer1   state.Bag[string]
	Buffer2   state.Bag[int64]
	Watermark timers.EventTime
}

func (s *stateAndTimersFn) ProcessElement(sp state.Provider, tp timers.Provider, w beam.Window, key string, value int64, emit func(string, int64)) error {
	// ... handle processing elements here, set a callback timer...

	// Read all the data from Buffer1 in this window.
	vals, ok, err := s.Buffer1.Read(sp)
	if err != nil {
		return err
	}
	if ok && s.shouldClearBuffer(vals) {
		// clear the buffer data if required conditions are met.
		s.Buffer1.Clear(sp)
	}

	// Add the value to Buffer2.
	s.Buffer2.Add(sp, value)

	if s.allConditionsMet() {
		// Clear the timer if certain condition met and you don't want to trigger
		// the callback method.
		s.Watermark.Clear(tp)
	}

	emit(key, value)

	return nil
}

func (s *stateAndTimersFn) OnTimer(sp state.Provider, tp timers.Provider, w beam.Window, key string, timer timers.Context, emit func(string, int64)) error {
	// Window and key parameters are really useful especially for debugging issues.
	switch timer.Family {
	case s.Watermark.Family:
		// timer expired, emit a different signal
		emit(key, -1)
	}
	return nil
}

func (s *stateAndTimersFn) shouldClearBuffer([]string) bool {
	// some business logic
	return false
}

func (s *stateAndTimersFn) allConditionsMet() bool {
	// other business logic
	return true
}

// [END state_and_timers]

// [START bag_state]

// bagStateFn only emits words that haven't been seen
type bagStateFn struct {
	Bag state.Bag[string]
}

func (s *bagStateFn) ProcessElement(p state.Provider, book, word string, emitWords func(string)) error {
	// Get all values we've written to this bag state in this window.
	vals, ok, err := s.Bag.Read(p)
	if err != nil {
		return err
	}
	if !ok || !contains(vals, word) {
		emitWords(word)
		s.Bag.Add(p, word)
	}

	if len(vals) > 10000 {
		// Example of clearing and starting again with an empty bag
		s.Bag.Clear(p)
	}

	return nil
}

// [END bag_state]

// [START value_state]

// valueStateFn keeps track of the number of elements seen.
type valueStateFn struct {
	Val state.Value[int]
}

func (s *valueStateFn) ProcessElement(p state.Provider, book string, word string, emitWords func(string)) error {
	// Get the value stored in our state
	val, ok, err := s.Val.Read(p)
	if err != nil {
		return err
	}
	if !ok {
		s.Val.Write(p, 1)
	} else {
		s.Val.Write(p, val+1)
	}

	if val > 10000 {
		// Example of clearing and starting again with an empty bag
		s.Val.Clear(p)
	}

	return nil
}

// [END value_state]

type MyCustomType struct{}

func (m MyCustomType) Bytes() []byte {
	return nil
}

func (m MyCustomType) FromBytes(_ []byte) MyCustomType {
	return m
}

// [START value_state_coder]

type valueStateDoFn struct {
	Val state.Value[MyCustomType]
}

func encode(m MyCustomType) []byte {
	return m.Bytes()
}

func decode(b []byte) MyCustomType {
	return MyCustomType{}.FromBytes(b)
}

func init() {
	beam.RegisterCoder(reflect.TypeOf((*MyCustomType)(nil)).Elem(), encode, decode)
}

// [END value_state_coder]

type combineFn struct{}

// [START combining_state]

// combiningStateFn keeps track of the number of elements seen.
type combiningStateFn struct {
	// types are the types of the accumulator, input, and output respectively
	Val state.Combining[int, int, int]
}

func (s *combiningStateFn) ProcessElement(p state.Provider, book string, word string, emitWords func(string)) error {
	// Get the value stored in our state
	val, _, err := s.Val.Read(p)
	if err != nil {
		return err
	}
	s.Val.Add(p, 1)

	if val > 10000 {
		// Example of clearing and starting again with an empty bag
		s.Val.Clear(p)
	}

	return nil
}

func combineState(s beam.Scope, input beam.PCollection) beam.PCollection {
	// ...
	// CombineFn param can be a simple fn like this or a structural CombineFn
	cFn := state.MakeCombiningState[int, int, int]("stateKey", func(a, b int) int {
		return a + b
	})
	combined := beam.ParDo(s, combiningStateFn{Val: cFn}, input)

	// ...

	// [END combining_state]

	return combined
}

// [START event_time_timer]

type eventTimerDoFn struct {
	State state.Value[int64]
	Timer timers.EventTime
}

func (fn *eventTimerDoFn) ProcessElement(ts beam.EventTime, sp state.Provider, tp timers.Provider, book, word string, emitWords func(string)) {
	// ...

	// Set an event-time timer to the element timestamp.
	fn.Timer.Set(tp, ts.ToTime())

	// ...
}

func (fn *eventTimerDoFn) OnTimer(sp state.Provider, tp timers.Provider, w beam.Window, key string, timer timers.Context, emitWords func(string)) {
	switch timer.Family {
	case fn.Timer.Family:
		// process callback for this timer
	}
}

func AddEventTimeDoFn(s beam.Scope, in beam.PCollection) beam.PCollection {
	return beam.ParDo(s, &eventTimerDoFn{
		// Timers are given family names so their callbacks can be handled independantly.
		Timer: timers.InEventTime("processWatermark"),
		State: state.MakeValueState[int64]("latest"),
	}, in)
}

// [END event_time_timer]

// [START processing_time_timer]

type processingTimerDoFn struct {
	Timer timers.ProcessingTime
}

func (fn *processingTimerDoFn) ProcessElement(sp state.Provider, tp timers.Provider, book, word string, emitWords func(string)) {
	// ...

	// Set a timer to go off 30 seconds in the future.
	fn.Timer.Set(tp, time.Now().Add(30*time.Second))

	// ...
}

func (fn *processingTimerDoFn) OnTimer(sp state.Provider, tp timers.Provider, w beam.Window, key string, timer timers.Context, emitWords func(string)) {
	switch timer.Family {
	case fn.Timer.Family:
		// process callback for this timer
	}
}

func AddProcessingTimeDoFn(s beam.Scope, in beam.PCollection) beam.PCollection {
	return beam.ParDo(s, &processingTimerDoFn{
		// Timers are given family names so their callbacks can be handled independantly.
		Timer: timers.InProcessingTime("timer"),
	}, in)
}

// [END processing_time_timer]

// [START dynamic_timer_tags]

type hasAction interface {
	Action() string
}

type dynamicTagsDoFn[V hasAction] struct {
	Timer timers.EventTime
}

func (fn *dynamicTagsDoFn[V]) ProcessElement(ts beam.EventTime, tp timers.Provider, key string, value V, emitWords func(string)) {
	// ...

	// Set a timer to go off 30 seconds in the future.
	fn.Timer.Set(tp, ts.ToTime(), timers.WithTag(value.Action()))

	// ...
}

func (fn *dynamicTagsDoFn[V]) OnTimer(tp timers.Provider, w beam.Window, key string, timer timers.Context, emitWords func(string)) {
	switch timer.Family {
	case fn.Timer.Family:
		tag := timer.Tag // Do something with fired tag
		_ = tag
	}
}

func AddDynamicTimerTagsDoFn[V hasAction](s beam.Scope, in beam.PCollection) beam.PCollection {
	return beam.ParDo(s, &dynamicTagsDoFn[V]{
		Timer: timers.InEventTime("actionTimers"),
	}, in)
}

// [END dynamic_timer_tags]

// [START timer_output_timestamps_bad]

type badTimerOutputTimestampsFn[V any] struct {
	ElementBag  state.Bag[V]
	TimerSet    state.Value[bool]
	OutputState timers.ProcessingTime
}

func (fn *badTimerOutputTimestampsFn[V]) ProcessElement(sp state.Provider, tp timers.Provider, key string, value V, emit func(string)) error {
	// Add the current element to the bag for this key.
	if err := fn.ElementBag.Add(sp, value); err != nil {
		return err
	}
	set, _, err := fn.TimerSet.Read(sp)
	if err != nil {
		return err
	}
	if !set {
		fn.OutputState.Set(tp, time.Now().Add(1*time.Minute))
		fn.TimerSet.Write(sp, true)
	}
	return nil
}

func (fn *badTimerOutputTimestampsFn[V]) OnTimer(sp state.Provider, tp timers.Provider, w beam.Window, key string, timer timers.Context, emit func(string)) error {
	switch timer.Family {
	case fn.OutputState.Family:
		vs, _, err := fn.ElementBag.Read(sp)
		if err != nil {
			return err
		}
		for _, v := range vs {
			// Output each element
			emit(fmt.Sprintf("%v", v))
		}

		fn.ElementBag.Clear(sp)
		// Note that the timer has now fired.
		fn.TimerSet.Clear(sp)
	}
	return nil
}

// [END timer_output_timestamps_bad]

// [START timer_output_timestamps_good]

type element[V any] struct {
	Timestamp int64
	Value     V
}

type goodTimerOutputTimestampsFn[V any] struct {
	ElementBag        state.Bag[element[V]]                // The bag of elements accumulated.
	TimerTimerstamp   state.Value[int64]                   // The timestamp of the timer set.
	MinTimestampInBag state.Combining[int64, int64, int64] // The minimum timestamp stored in the bag.
	OutputState       timers.ProcessingTime                // The timestamp of the timer.
}

func (fn *goodTimerOutputTimestampsFn[V]) ProcessElement(et beam.EventTime, sp state.Provider, tp timers.Provider, key string, value V, emit func(beam.EventTime, string)) error {
	// ...
	// Add the current element to the bag for this key, and preserve the event time.
	if err := fn.ElementBag.Add(sp, element[V]{Timestamp: et.Milliseconds(), Value: value}); err != nil {
		return err
	}

	// Keep track of the minimum element timestamp currently stored in the bag.
	fn.MinTimestampInBag.Add(sp, et.Milliseconds())

	// If the timer is already set, then reset it at the same time but with an updated output timestamp (otherwise
	// we would keep resetting the timer to the future). If there is no timer set, then set one to expire in a minute.
	ts, ok, _ := fn.TimerTimerstamp.Read(sp)
	var tsToSet time.Time
	if ok {
		tsToSet = time.UnixMilli(ts)
	} else {
		tsToSet = time.Now().Add(1 * time.Minute)
	}

	minTs, _, _ := fn.MinTimestampInBag.Read(sp)
	outputTs := time.UnixMilli(minTs)

	// Setting the outputTimestamp to the minimum timestamp in the bag holds the watermark to that timestamp until the
	// timer fires. This allows outputting all the elements with their timestamp.
	fn.OutputState.Set(tp, tsToSet, timers.WithOutputTimestamp(outputTs))
	fn.TimerTimerstamp.Write(sp, tsToSet.UnixMilli())

	return nil
}

func (fn *goodTimerOutputTimestampsFn[V]) OnTimer(sp state.Provider, tp timers.Provider, w beam.Window, key string, timer timers.Context, emit func(beam.EventTime, string)) error {
	switch timer.Family {
	case fn.OutputState.Family:
		vs, _, err := fn.ElementBag.Read(sp)
		if err != nil {
			return err
		}
		for _, v := range vs {
			// Output each element with their timestamp
			emit(beam.EventTime(v.Timestamp), fmt.Sprintf("%v", v.Value))
		}

		fn.ElementBag.Clear(sp)
		// Note that the timer has now fired.
		fn.TimerTimerstamp.Clear(sp)
	}
	return nil
}

func AddTimedOutputBatching[V any](s beam.Scope, in beam.PCollection) beam.PCollection {
	return beam.ParDo(s, &goodTimerOutputTimestampsFn[V]{
		ElementBag:      state.MakeBagState[element[V]]("elementBag"),
		TimerTimerstamp: state.MakeValueState[int64]("timerTimestamp"),
		MinTimestampInBag: state.MakeCombiningState[int64, int64, int64]("minTimestampInBag", func(a, b int64) int64 {
			if a < b {
				return a
			}
			return b
		}),
		OutputState: timers.InProcessingTime("outputState"),
	}, in)
}

// [END timer_output_timestamps_good]

// updateState exists for example purposes only
func updateState(sp, state, k, v any) {}

// [START timer_garbage_collection]

type timerGarbageCollectionFn[V any] struct {
	State             state.Value[V]                       // The state for the key.
	MaxTimestampInBag state.Combining[int64, int64, int64] // The maximum element timestamp seen so far.
	GcTimer           timers.EventTime                     // The timestamp of the timer.
}

func (fn *timerGarbageCollectionFn[V]) ProcessElement(et beam.EventTime, sp state.Provider, tp timers.Provider, key string, value V, emit func(beam.EventTime, string)) {
	updateState(sp, fn.State, key, value)
	fn.MaxTimestampInBag.Add(sp, et.Milliseconds())

	// Set the timer to be one hour after the maximum timestamp seen. This will keep overwriting the same timer, so
	// as long as there is activity on this key the state will stay active. Once the key goes inactive for one hour's
	// worth of event time (as measured by the watermark), then the gc timer will fire.
	maxTs, _, _ := fn.MaxTimestampInBag.Read(sp)
	expirationTime := time.UnixMilli(maxTs).Add(1 * time.Hour)
	fn.GcTimer.Set(tp, expirationTime)
}

func (fn *timerGarbageCollectionFn[V]) OnTimer(sp state.Provider, tp timers.Provider, w beam.Window, key string, timer timers.Context, emit func(beam.EventTime, string)) {
	switch timer.Family {
	case fn.GcTimer.Family:
		// Clear all the state for the key
		fn.State.Clear(sp)
		fn.MaxTimestampInBag.Clear(sp)
	}
}

func AddTimerGarbageCollection[V any](s beam.Scope, in beam.PCollection) beam.PCollection {
	return beam.ParDo(s, &timerGarbageCollectionFn[V]{
		State: state.MakeValueState[V]("timerTimestamp"),
		MaxTimestampInBag: state.MakeCombiningState[int64, int64, int64]("maxTimestampInBag", func(a, b int64) int64 {
			if a > b {
				return a
			}
			return b
		}),
		GcTimer: timers.InEventTime("gcTimer"),
	}, in)
}

// [END timer_garbage_collection]

type Event struct{}

func (*Event) isClick() bool { return false }

// [START join_dofn_example]

type JoinedEvent struct {
	View, Click *Event
}

type joinDoFn struct {
	View  state.Value[*Event] // Store the view event.
	Click state.Value[*Event] // Store the click event.

	MaxTimestampSeen state.Combining[int64, int64, int64] // The maximum element timestamp seen so far.
	GcTimer          timers.EventTime                     // The timestamp of the timer.
}

func (fn *joinDoFn) ProcessElement(et beam.EventTime, sp state.Provider, tp timers.Provider, key string, event *Event, emit func(JoinedEvent)) {
	valueState := fn.View
	if event.isClick() {
		valueState = fn.Click
	}
	valueState.Write(sp, event)

	view, _, _ := fn.View.Read(sp)
	click, _, _ := fn.Click.Read(sp)
	if view != nil && click != nil {
		emit(JoinedEvent{View: view, Click: click})
		fn.clearState(sp)
		return
	}

	fn.MaxTimestampSeen.Add(sp, et.Milliseconds())
	expTs, _, _ := fn.MaxTimestampSeen.Read(sp)
	fn.GcTimer.Set(tp, time.UnixMilli(expTs).Add(1*time.Hour))
}

func (fn *joinDoFn) OnTimer(sp state.Provider, tp timers.Provider, w beam.Window, key string, timer timers.Context, emit func(beam.EventTime, string)) {
	switch timer.Family {
	case fn.GcTimer.Family:
		fn.clearState(sp)
	}
}

func (fn *joinDoFn) clearState(sp state.Provider) {
	fn.View.Clear(sp)
	fn.Click.Clear(sp)
	fn.MaxTimestampSeen.Clear(sp)
}

func AddJoinDoFn(s beam.Scope, in beam.PCollection) beam.PCollection {
	return beam.ParDo(s, &joinDoFn{
		View:  state.MakeValueState[*Event]("view"),
		Click: state.MakeValueState[*Event]("click"),
		MaxTimestampSeen: state.MakeCombiningState[int64, int64, int64]("maxTimestampSeen", func(a, b int64) int64 {
			if a > b {
				return a
			}
			return b
		}),
		GcTimer: timers.InEventTime("gcTimer"),
	}, in)
}

// [END join_dofn_example]

func sendRpc(...any) {}

// [START batching_dofn_example]

type bufferDoFn[V any] struct {
	Elements   state.Bag[V]      // Store the elements buffered so far.
	IsTimerSet state.Value[bool] // Keep track of whether a timer is currently set or not.

	OutputElements timers.ProcessingTime // The processing-time timer user to publish the RPC.
}

func (fn *bufferDoFn[V]) ProcessElement(et beam.EventTime, sp state.Provider, tp timers.Provider, key string, value V) {
	fn.Elements.Add(sp, value)

	isSet, _, _ := fn.IsTimerSet.Read(sp)
	if !isSet {
		fn.OutputElements.Set(tp, time.Now().Add(10*time.Second))
		fn.IsTimerSet.Write(sp, true)
	}
}

func (fn *bufferDoFn[V]) OnTimer(sp state.Provider, tp timers.Provider, w beam.Window, key string, timer timers.Context) {
	switch timer.Family {
	case fn.OutputElements.Family:
		elements, _, _ := fn.Elements.Read(sp)
		sendRpc(elements)
		fn.Elements.Clear(sp)
		fn.IsTimerSet.Clear(sp)
	}
}

func AddBufferDoFn[V any](s beam.Scope, in beam.PCollection) beam.PCollection {
	return beam.ParDo(s, &bufferDoFn[V]{
		Elements:   state.MakeBagState[V]("elements"),
		IsTimerSet: state.MakeValueState[bool]("isTimerSet"),

		OutputElements: timers.InProcessingTime("outputElements"),
	}, in)
}

// [END batching_dofn_example]

type statefulDoFn struct {
	S state.Value[int]
}

func statefulPipeline() beam.PCollection {
	var s beam.Scope
	var elements beam.PCollection

	// [START windowed_state]

	items := beam.ParDo(s, statefulDoFn{
		S: state.MakeValueState[int]("S"),
	}, elements)
	out := beam.WindowInto(s, window.NewFixedWindows(24*time.Hour), items)

	// [END windowed_state]

	return out
}

func init() {
	register.Function3x0(extractWordsFn)
	// 1 input of type string => Emitter1[string]
	register.Emitter1[string]()
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
