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

package internal

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/google/go-cmp/cmp"
)

// The Test DoFns live outside of the test files to get coverage information on DoFn
// Lifecycle method execution. This inflates binary size, but ensures the runner is
// exercising the expected feature set.
//
// Once there's enough confidence in the runner, we can move these into a dedicated testing
// package along with the pipelines that use them.

// Registrations should happen in the test files, so the compiler can prune these
// when they are not in use.

func dofn1(imp []byte, emit func(int64)) {
	emit(1)
	emit(2)
	emit(3)
}

func dofn1kv(imp []byte, emit func(int64, int64)) {
	emit(0, 1)
	emit(0, 2)
	emit(0, 3)
}

func dofn1x2(imp []byte, emitA func(int64), emitB func(int64)) {
	emitA(1)
	emitA(2)
	emitA(3)
	emitB(4)
	emitB(5)
	emitB(6)
}

func dofn1x5(imp []byte, emitA, emitB, emitC, emitD, emitE func(int64)) {
	emitA(1)
	emitB(2)
	emitC(3)
	emitD(4)
	emitE(5)
	emitA(6)
	emitB(7)
	emitC(8)
	emitD(9)
	emitE(10)
}

func dofn2x1(imp []byte, iter func(*int64) bool, emit func(int64)) {
	var v, sum, c int64
	for iter(&v) {
		fmt.Println("dofn2x1 v", v, " c ", c)
		sum += v
		c++
	}
	fmt.Println("dofn2x1 sum", sum, "count", c)
	emit(sum)
}

func dofn2x2KV(imp []byte, iter func(*string, *int64) bool, emitK func(string), emitV func(int64)) {
	var k string
	var v, sum int64
	for iter(&k, &v) {
		sum += v
		emitK(k)
	}
	emitV(sum)
}

func dofnMultiMap(key string, lookup func(string) func(*int64) bool, emitK func(string), emitV func(int64)) {
	var v, sum int64
	iter := lookup(key)
	for iter(&v) {
		sum += v
	}
	emitK(key)
	emitV(sum)
}

func dofn3x1(sum int64, iter1, iter2 func(*int64) bool, emit func(int64)) {
	var v int64
	for iter1(&v) {
		sum += v
	}
	for iter2(&v) {
		sum += v
	}
	emit(sum)
}

// int64Check validates that within a single bundle, for each window,
// we received the expected int64 values & sends them downstream.
//
// Invalid pattern for general testing, as it will fail
// on other valid execution patterns, like single element bundles.
type int64Check struct {
	Name string
	Want []int
	got  map[beam.Window][]int
}

func (fn *int64Check) StartBundle(_ func(int64)) error {
	fn.got = map[beam.Window][]int{}
	return nil
}

func (fn *int64Check) ProcessElement(w beam.Window, v int64, _ func(int64)) {
	fn.got[w] = append(fn.got[w], int(v))
}

func (fn *int64Check) FinishBundle(_ func(int64)) error {
	sort.Ints(fn.Want)
	// Check for each window individually.
	for _, vs := range fn.got {
		sort.Ints(vs)
		if d := cmp.Diff(fn.Want, vs); d != "" {
			return fmt.Errorf("int64Check[%v] (-want, +got): %v", fn.Name, d)
		}
		// Clear for subsequent calls.
	}
	fn.got = nil
	return nil
}

// stringCheck validates that within a single bundle,
// we received the expected string values.
// Re-emits them downstream.
//
// Invalid pattern for general testing, as it will fail
// on other valid execution patterns, like single element bundles.
type stringCheck struct {
	Name string
	Want []string
	got  []string
}

func (fn *stringCheck) ProcessElement(v string, _ func(string)) {
	fn.got = append(fn.got, v)
}

func (fn *stringCheck) FinishBundle(_ func(string)) error {
	sort.Strings(fn.got)
	sort.Strings(fn.Want)
	if d := cmp.Diff(fn.Want, fn.got); d != "" {
		return fmt.Errorf("stringCheck[%v] (-want, +got): %v", fn.Name, d)
	}
	return nil
}

func dofn2(v int64, emit func(int64)) {
	emit(v + 1)
}

func dofnKV(imp []byte, emit func(string, int64)) {
	emit("a", 1)
	emit("b", 2)
	emit("a", 3)
	emit("b", 4)
	emit("a", 5)
	emit("b", 6)
}

func dofnKV2(imp []byte, emit func(int64, string)) {
	emit(1, "a")
	emit(2, "b")
	emit(1, "a")
	emit(2, "b")
	emit(1, "a")
	emit(2, "b")
}

func dofnGBK(k string, vs func(*int64) bool, emit func(int64)) {
	var v, sum int64
	for vs(&v) {
		sum += v
	}
	emit(sum)
}

func dofnGBK2(k int64, vs func(*string) bool, emit func(string)) {
	var v, sum string
	for vs(&v) {
		sum += v
	}
	emit(sum)
}

type testRow struct {
	A string
	B int64
}

func dofnKV3(imp []byte, emit func(testRow, testRow)) {
	emit(testRow{"a", 1}, testRow{"a", 1})
}

func dofnGBK3(k testRow, vs func(*testRow) bool, emit func(string)) {
	var v testRow
	vs(&v)
	emit(fmt.Sprintf("%v: %v", k, v))
}

const (
	ns = "localtest"
)

func dofnSink(ctx context.Context, _ []byte) {
	beam.NewCounter(ns, "sunk").Inc(ctx, 73)
}

func dofn1Counter(ctx context.Context, _ []byte, emit func(int64)) {
	beam.NewCounter(ns, "count").Inc(ctx, 1)
}

func doFnFail(ctx context.Context, _ []byte, emit func(int64)) error {
	beam.NewCounter(ns, "count").Inc(ctx, 1)
	return fmt.Errorf("doFnFail: failing as intended")
}

func combineIntSum(a, b int64) int64 {
	return a + b
}

// SourceConfig is a struct containing all the configuration options for a
// synthetic source. It should be created via a SourceConfigBuilder, not by
// directly initializing it (the fields are public to allow encoding).
type SourceConfig struct {
	NumElements   int64 `json:"num_records" beam:"num_records"`
	InitialSplits int64 `json:"initial_splits" beam:"initial_splits"`
}

// intRangeFn is a splittable DoFn for counting from 1 to N.
type intRangeFn struct{}

// CreateInitialRestriction creates an offset range restriction representing
// the number of elements to emit.
func (fn *intRangeFn) CreateInitialRestriction(config SourceConfig) offsetrange.Restriction {
	return offsetrange.Restriction{
		Start: 0,
		End:   int64(config.NumElements),
	}
}

// SplitRestriction splits restrictions equally according to the number of
// initial splits specified in SourceConfig. Each restriction output by this
// method will contain at least one element, so the number of splits will not
// exceed the number of elements.
func (fn *intRangeFn) SplitRestriction(config SourceConfig, rest offsetrange.Restriction) (splits []offsetrange.Restriction) {
	return rest.EvenSplits(int64(config.InitialSplits))
}

// RestrictionSize outputs the size of the restriction as the number of elements
// that restriction will output.
func (fn *intRangeFn) RestrictionSize(_ SourceConfig, rest offsetrange.Restriction) float64 {
	return rest.Size()
}

// CreateTracker just creates an offset range restriction tracker for the
// restriction.
func (fn *intRangeFn) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
}

// ProcessElement creates a number of random elements based on the restriction
// tracker received. Each element is a random byte slice key and value, in the
// form of KV<[]byte, []byte>.
func (fn *intRangeFn) ProcessElement(rt *sdf.LockRTracker, config SourceConfig, emit func(int64)) error {
	for i := rt.GetRestriction().(offsetrange.Restriction).Start; rt.TryClaim(i); i++ {
		// Add 1 since the restrictions are from [0 ,N), but we want [1, N]
		emit(i + 1)
	}
	return nil
}

func init() {
	register.DoFn3x1[*sdf.LockRTracker, []byte, func(int64), sdf.ProcessContinuation](&selfCheckpointingDoFn{})
	register.Emitter1[int64]()
}

type selfCheckpointingDoFn struct{}

// CreateInitialRestriction creates the restriction being used by the SDF. In this case, the range
// of values produced by the restriction is [Start, End).
func (fn *selfCheckpointingDoFn) CreateInitialRestriction(_ []byte) offsetrange.Restriction {
	return offsetrange.Restriction{
		Start: int64(0),
		End:   int64(10),
	}
}

// CreateTracker wraps the given restriction into a LockRTracker type.
func (fn *selfCheckpointingDoFn) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
}

// RestrictionSize returns the size of the current restriction
func (fn *selfCheckpointingDoFn) RestrictionSize(_ []byte, rest offsetrange.Restriction) float64 {
	return rest.Size()
}

// SplitRestriction modifies the offsetrange.Restriction's sized restriction function to produce a size-zero restriction
// at the end of execution.
func (fn *selfCheckpointingDoFn) SplitRestriction(_ []byte, rest offsetrange.Restriction) []offsetrange.Restriction {
	size := int64(3)
	s := rest.Start
	var splits []offsetrange.Restriction
	for e := s + size; e <= rest.End; s, e = e, e+size {
		splits = append(splits, offsetrange.Restriction{Start: s, End: e})
	}
	splits = append(splits, offsetrange.Restriction{Start: s, End: rest.End})
	return splits
}

// ProcessElement continually gets the start position of the restriction and emits it as an int64 value before checkpointing.
// This causes the restriction to be split after the claimed work and produce no primary roots.
func (fn *selfCheckpointingDoFn) ProcessElement(rt *sdf.LockRTracker, _ []byte, emit func(int64)) sdf.ProcessContinuation {
	position := rt.GetRestriction().(offsetrange.Restriction).Start

	for {
		if rt.TryClaim(position) {
			// Successful claim, emit the value and move on.
			emit(position)
			position++
		} else if rt.GetError() != nil || rt.IsDone() {
			// Stop processing on error or completion
			if err := rt.GetError(); err != nil {
				log.Errorf(context.Background(), "error in restriction tracker, got %v", err)
			}
			return sdf.StopProcessing()
		} else {
			// Resume later.
			return sdf.ResumeProcessingIn(5 * time.Second)
		}
	}
}
