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

package graph

import (
	"context"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

func TestNewDoFn(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		tests := []struct {
			dfn interface{}
			opt func(*config)
		}{
			{dfn: func(string) int { return 0 }, opt: NumMainInputs(MainSingle)},
			{dfn: func(string, int) int { return 0 }, opt: NumMainInputs(MainKv)},
			{dfn: func(context.Context, typex.Window, typex.EventTime, reflect.Type, string, int, func(*int) bool, func() func(*int) bool, func(int)) (typex.EventTime, int, error) {
				return 0, 0, nil
			}, opt: NumMainInputs(MainKv)},
			{dfn: &GoodDoFn{}, opt: NumMainInputs(MainSingle)},
			{dfn: &GoodDoFnOmittedMethods{}, opt: NumMainInputs(MainSingle)},
			{dfn: &GoodDoFnEmits{}, opt: NumMainInputs(MainSingle)},
			{dfn: &GoodDoFnSideInputs{}, opt: NumMainInputs(MainSingle)},
			{dfn: &GoodDoFnKv{}, opt: NumMainInputs(MainKv)},
			{dfn: &GoodDoFnKvSideInputs{}, opt: NumMainInputs(MainKv)},
			{dfn: &GoodDoFnAllExtras{}, opt: NumMainInputs(MainKv)},
			{dfn: &GoodDoFnUnexportedExtraMethod{}, opt: NumMainInputs(MainSingle)},
			{dfn: &GoodDoFnCoGbk1{}, opt: NumMainInputs(MainKv)},
			{dfn: &GoodDoFnCoGbk2{}, opt: CoGBKMainInput(3)},
			{dfn: &GoodDoFnCoGbk7{}, opt: CoGBKMainInput(8)},
			{dfn: &GoodDoFnCoGbk1wSide{}, opt: NumMainInputs(MainKv)},
		}

		for _, test := range tests {
			t.Run(reflect.TypeOf(test.dfn).String(), func(t *testing.T) {
				// Valid DoFns should pass validation with and without KV info.
				if _, err := NewDoFn(test.dfn); err != nil {
					t.Fatalf("NewDoFn failed: %v", err)
				}
				if _, err := NewDoFn(test.dfn, test.opt); err != nil {
					cfg := defaultConfig()
					test.opt(cfg)
					t.Fatalf("NewDoFn(%#v) failed: %v", cfg, err)
				}
			})
		}
	})
	t.Run("invalid", func(t *testing.T) {
		tests := []struct {
			dfn interface{}
		}{
			// Validate main inputs.
			{dfn: func() int { return 0 }}, // No inputs.
			{dfn: func(func(*int) bool, int) int { // Side input before main input.
				return 0
			}},
			{dfn: &BadDoFnHasRTracker{}},
			// Validate emit parameters.
			{dfn: &BadDoFnNoEmitsStartBundle{}},
			{dfn: &BadDoFnMissingEmitsStartBundle{}},
			{dfn: &BadDoFnMismatchedEmitsStartBundle{}},
			{dfn: &BadDoFnNoEmitsFinishBundle{}},
			// Validate side inputs.
			{dfn: &BadDoFnMissingSideInputsStartBundle{}},
			{dfn: &BadDoFnMismatchedSideInputsStartBundle{}},
			// Validate setup/teardown.
			{dfn: &BadDoFnParamsInSetup{}},
			{dfn: &BadDoFnParamsInTeardown{}},
			// Validate return values.
			{dfn: &BadDoFnReturnValuesInStartBundle{}},
			{dfn: &BadDoFnReturnValuesInFinishBundle{}},
			{dfn: &BadDoFnReturnValuesInSetup{}},
			{dfn: &BadDoFnReturnValuesInTeardown{}},
		}
		for _, test := range tests {
			t.Run(reflect.TypeOf(test.dfn).String(), func(t *testing.T) {
				if cfn, err := NewDoFn(test.dfn); err != nil {
					t.Logf("NewDoFn failed as expected:\n%v", err)
				} else {
					t.Errorf("NewDoFn(%v) = %v, want failure", cfn.Name(), cfn)
				}
				// If validation fails with unknown main inputs, then it should
				// always fail for any known number of main inputs, so test them
				// all. Error messages won't necessarily match.
				if cfn, err := NewDoFn(test.dfn, NumMainInputs(MainSingle)); err != nil {
					t.Logf("NewDoFn failed as expected:\n%v", err)
				} else {
					t.Errorf("NewDoFn(%v, NumMainInputs(MainSingle)) = %v, want failure", cfn.Name(), cfn)
				}
				if cfn, err := NewDoFn(test.dfn, NumMainInputs(MainKv)); err != nil {
					t.Logf("NewDoFn failed as expected:\n%v", err)
				} else {
					t.Errorf("NewDoFn(%v, NumMainInputs(MainKv)) = %v, want failure", cfn.Name(), cfn)
				}
			})
		}
	})
	// Tests ambiguous situations that pass DoFn validation when number of main
	// inputs is unknown, but fails when it's specified.
	t.Run("invalidWithKnownKvs", func(t *testing.T) {
		tests := []struct {
			dfn  interface{}
			main mainInputs
		}{
			{dfn: func(int) int { return 0 }, main: MainKv}, // Not enough inputs.
			{dfn: func(int, func(int)) int { // Emit before all main inputs.
				return 0
			}, main: MainKv},
			{dfn: &BadDoFnAmbiguousMainInput{}, main: MainKv},
			{dfn: &BadDoFnAmbiguousSideInput{}, main: MainSingle},
			// These are ambiguous with CoGBKs, but should fail with known MainInputs.
			{dfn: &BadDoFnNoSideInputsStartBundle{}, main: MainSingle},
			{dfn: &BadDoFnNoSideInputsStartBundle{}, main: MainKv},
			{dfn: &BadDoFnNoSideInputsFinishBundle{}, main: MainSingle},
			{dfn: &BadDoFnNoSideInputsFinishBundle{}, main: MainKv},
		}
		for _, test := range tests {
			t.Run(reflect.TypeOf(test.dfn).String(), func(t *testing.T) {
				// These tests should be ambiguous enough to pass NewDoFn. If
				// validation improves and they start failing, move the test
				// cases to "invalid".
				if _, err := NewDoFn(test.dfn); err != nil {
					t.Fatalf("NewDoFn failed: %v", err)
				}
				if cfn, err := NewDoFn(test.dfn, NumMainInputs(test.main)); err != nil {
					t.Logf("NewDoFn failed as expected:\n%v", err)
				} else {
					t.Errorf("NewDoFn(%v, NumMainInputs(%v)) = %v, want failure", cfn.Name(), test.main, cfn)
				}
			})
		}
	})
}

func TestNewDoFnSdf(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		tests := []struct {
			dfn  interface{}
			main mainInputs
		}{
			{dfn: &GoodSdf{}, main: MainSingle},
			{dfn: &GoodSdfKv{}, main: MainKv},
		}

		for _, test := range tests {
			t.Run(reflect.TypeOf(test.dfn).String(), func(t *testing.T) {
				// Valid DoFns should pass validation with and without KV info.
				if _, err := NewDoFn(test.dfn); err != nil {
					t.Fatalf("NewDoFn with SDF failed: %v", err)
				}
				if _, err := NewDoFn(test.dfn, NumMainInputs(test.main)); err != nil {
					t.Fatalf("NewDoFn(NumMainInputs(%v)) with SDF failed: %v", test.main, err)
				}
			})
		}
	})
	t.Run("invalid", func(t *testing.T) {
		tests := []struct {
			dfn interface{}
		}{
			// Validate missing SDF methods cause errors.
			{dfn: &BadSdfMissingMethods{}},
			// Validate param numbers.
			{dfn: &BadSdfParamsCreateRest{}},
			{dfn: &BadSdfParamsSplitRest{}},
			{dfn: &BadSdfParamsRestSize{}},
			{dfn: &BadSdfParamsCreateTracker{}},
			// Validate return numbers.
			{dfn: &BadSdfReturnsCreateRest{}},
			{dfn: &BadSdfReturnsSplitRest{}},
			{dfn: &BadSdfReturnsRestSize{}},
			{dfn: &BadSdfReturnsCreateTracker{}},
			// Validate element types consistent with ProcessElement.
			{dfn: &BadSdfElementTCreateRest{}},
			{dfn: &BadSdfElementTSplitRest{}},
			{dfn: &BadSdfElementTRestSize{}},
			// Validate restriction type consistent with CreateRestriction.
			{dfn: &BadSdfRestTSplitRestParam{}},
			{dfn: &BadSdfRestTSplitRestReturn{}},
			{dfn: &BadSdfRestTRestSize{}},
			{dfn: &BadSdfRestTCreateTracker{}},
			// Validate other types
			{dfn: &BadSdfRestSizeReturn{}},
			{dfn: &BadSdfCreateTrackerReturn{}},
			{dfn: &BadSdfMismatchedRTracker{}},
			{dfn: &BadSdfMissingRTracker{}},
		}
		for _, test := range tests {
			t.Run(reflect.TypeOf(test.dfn).String(), func(t *testing.T) {
				if cfn, err := NewDoFn(test.dfn); err != nil {
					t.Logf("NewDoFn with SDF failed as expected:\n%v", err)
				} else {
					t.Errorf("NewDoFn(%v) = %v, want failure", cfn.Name(), cfn)
				}
				// If validation fails with unknown main inputs, then it should
				// always fail for any known number of main inputs, so test them
				// all. Error messages won't necessarily match.
				if cfn, err := NewDoFn(test.dfn, NumMainInputs(MainSingle)); err != nil {
					t.Logf("NewDoFn(NumMainInputs(MainSingle)) with SDF failed as expected:\n%v", err)
				} else {
					t.Errorf("NewDoFn(%v, NumMainInputs(MainSingle)) = %v, want failure", cfn.Name(), cfn)
				}
				if cfn, err := NewDoFn(test.dfn, NumMainInputs(MainKv)); err != nil {
					t.Logf("NewDoFn(NumMainInputs(MainKv)) with SDF failed as expected:\n%v", err)
				} else {
					t.Errorf("NewDoFn(%v, NumMainInputs(MainKv)) = %v, want failure", cfn.Name(), cfn)
				}
			})
		}
	})
}

func TestNewCombineFn(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		tests := []struct {
			cfn interface{}
		}{
			{cfn: func(int, int) int { return 0 }},
			{cfn: func(string, string) string { return "" }},
			{cfn: func(MyAccum, MyAccum) MyAccum { return MyAccum{} }},
			{cfn: func(MyAccum, MyAccum) (MyAccum, error) { return MyAccum{}, nil }},
			{cfn: func(context.Context, MyAccum, MyAccum) MyAccum { return MyAccum{} }},
			{cfn: func(context.Context, MyAccum, MyAccum) (MyAccum, error) { return MyAccum{}, nil }},
			{cfn: &GoodCombineFn{}},
			{cfn: &GoodWErrorCombineFn{}},
			{cfn: &GoodWContextCombineFn{}},
			{cfn: &GoodCombineFnUnexportedExtraMethod{}},
		}

		for _, test := range tests {
			t.Run(reflect.TypeOf(test.cfn).String(), func(t *testing.T) {
				if _, err := NewCombineFn(test.cfn); err != nil {
					t.Fatalf("NewCombineFn failed: %v", err)
				}
			})
		}
	})
	t.Run("invalid", func(t *testing.T) {
		tests := []struct {
			cfn interface{}
		}{
			// Validate MergeAccumulator errors
			{cfn: func() int { return 0 }},
			{cfn: func(int, int) {}},
			{cfn: func(int, int) string { return "" }},
			{cfn: func(string, string) int { return 0 }},
			{cfn: func(int, string) int { return 0 }},
			{cfn: func(string, int) int { return 0 }},
			{cfn: func(string, int) (int, error) { return 0, nil }},
			{cfn: &BadCombineFnNoMergeAccumulators{}},
			{cfn: &BadCombineFnNonBinaryMergeAccumulators{}},
			// Validate accumulator type mismatches
			{cfn: &BadCombineFnMisMatchedCreateAccumulator{}},
			{cfn: &BadCombineFnMisMatchedAddInputIn{}},
			{cfn: &BadCombineFnMisMatchedAddInputOut{}},
			{cfn: &BadCombineFnMisMatchedAddInputBoth{}},
			{cfn: &BadCombineFnMisMatchedExtractOutput{}},
			// Validate signatures
			{cfn: &BadCombineFnInvalidCreateAccumulator1{}},
			{cfn: &BadCombineFnInvalidCreateAccumulator2{}},
			{cfn: &BadCombineFnInvalidCreateAccumulator3{}},
			{cfn: &BadCombineFnInvalidCreateAccumulator4{}},
			{cfn: &BadCombineFnInvalidAddInput1{}},
			{cfn: &BadCombineFnInvalidAddInput2{}},
			{cfn: &BadCombineFnInvalidAddInput3{}},
			{cfn: &BadCombineFnInvalidAddInput4{}},
			{cfn: &BadCombineFnInvalidExtractOutput1{}},
			{cfn: &BadCombineFnInvalidExtractOutput2{}},
			{cfn: &BadCombineFnInvalidExtractOutput3{}},
			{cfn: &BadCombineFnExtraExportedMethod{}},
		}
		for _, test := range tests {
			t.Run(reflect.TypeOf(test.cfn).String(), func(t *testing.T) {
				if cfn, err := NewCombineFn(test.cfn); err != nil {
					// Note to Developer: To work on improving the error messages, use t.Errorf instead!
					t.Logf("NewCombineFn failed as expected:\n%v", err)
				} else {
					t.Errorf("AsCombineFn(%v) = %v, want failure", cfn.Name(), cfn)
				}
			})
		}
	})
}

// Do not copy. The following types are for testing signatures only.
// They are not working examples.
// Keep all test functions Above this point.

// Examples of correct DoFn signatures

type GoodDoFn struct{}

func (fn *GoodDoFn) ProcessElement(int) int {
	return 0
}

func (fn *GoodDoFn) StartBundle() {
}

func (fn *GoodDoFn) FinishBundle() {
}

func (fn *GoodDoFn) Setup() {
}

func (fn *GoodDoFn) Teardown() {
}

type GoodDoFnOmittedMethods struct{}

func (fn *GoodDoFnOmittedMethods) ProcessElement(int) int {
	return 0
}

type GoodDoFnEmits struct{}

func (fn *GoodDoFnEmits) ProcessElement(int, func(int), func(string)) int {
	return 0
}

func (fn *GoodDoFnEmits) StartBundle(func(int), func(string)) {
}

func (fn *GoodDoFnEmits) FinishBundle(func(int), func(string)) {
}

type GoodDoFnSideInputs struct{}

func (fn *GoodDoFnSideInputs) ProcessElement(int, func(*int) bool, string, func() func(*int) bool) int {
	return 0
}

func (fn *GoodDoFnSideInputs) StartBundle(func(*int) bool, string, func() func(*int) bool) {
}

func (fn *GoodDoFnSideInputs) FinishBundle(func(*int) bool, string, func() func(*int) bool) {
}

type GoodDoFnKv struct{}

func (fn *GoodDoFnKv) ProcessElement(int, int) int {
	return 0
}

func (fn *GoodDoFnKv) StartBundle() {
}

func (fn *GoodDoFnKv) FinishBundle() {
}

type GoodDoFnKvSideInputs struct{}

func (fn *GoodDoFnKvSideInputs) ProcessElement(int, int, string, func(*int) bool, func() func(*int) bool) int {
	return 0
}

func (fn *GoodDoFnKvSideInputs) StartBundle(string, func(*int) bool, func() func(*int) bool) {
}

func (fn *GoodDoFnKvSideInputs) FinishBundle(string, func(*int) bool, func() func(*int) bool) {
}

type GoodDoFnCoGbk1 struct{}

func (fn *GoodDoFnCoGbk1) ProcessElement(int, func(*string) bool) int {
	return 0
}

func (fn *GoodDoFnCoGbk1) StartBundle() {
}

func (fn *GoodDoFnCoGbk1) FinishBundle() {
}

type GoodDoFnCoGbk2 struct{}

func (fn *GoodDoFnCoGbk2) ProcessElement(int, func(*int) bool, func(*string) bool) int {
	return 0
}

func (fn *GoodDoFnCoGbk2) StartBundle() {
}

func (fn *GoodDoFnCoGbk2) FinishBundle() {
}

type GoodDoFnCoGbk7 struct{}

func (fn *GoodDoFnCoGbk7) ProcessElement(k int, v1, v2, v3, v4, v5, v6, v7 func(*int) bool) int {
	return 0
}

func (fn *GoodDoFnCoGbk7) StartBundle() {
}

func (fn *GoodDoFnCoGbk7) FinishBundle() {
}

type GoodDoFnCoGbk1wSide struct{}

func (fn *GoodDoFnCoGbk1wSide) ProcessElement(int, func(*string) bool, func(*int) bool) int {
	return 0
}

func (fn *GoodDoFnCoGbk1wSide) StartBundle(func(*int) bool) {
}

func (fn *GoodDoFnCoGbk1wSide) FinishBundle(func(*int) bool) {
}

type GoodDoFnAllExtras struct{}

func (fn *GoodDoFnAllExtras) ProcessElement(context.Context, typex.Window, typex.EventTime, reflect.Type, string, int, func(*int) bool, func() func(*int) bool, func(int)) (typex.EventTime, int, error) {
	return 0, 0, nil
}

func (fn *GoodDoFnAllExtras) StartBundle(context.Context, func(*int) bool, func() func(*int) bool, func(int)) {
}

func (fn *GoodDoFnAllExtras) FinishBundle(context.Context, func(*int) bool, func() func(*int) bool, func(int)) {
}

func (fn *GoodDoFnAllExtras) Setup(context.Context) error {
	return nil
}

func (fn *GoodDoFnAllExtras) Teardown(context.Context) error {
	return nil
}

type GoodDoFnUnexportedExtraMethod struct{}

func (fn *GoodDoFnUnexportedExtraMethod) ProcessElement(int) int {
	return 0
}

func (fn *GoodDoFnUnexportedExtraMethod) StartBundle() {
}

func (fn *GoodDoFnUnexportedExtraMethod) FinishBundle() {
}

func (fn *GoodDoFnUnexportedExtraMethod) Setup() {
}

func (fn *GoodDoFnUnexportedExtraMethod) Teardown() {
}

func (fn *GoodDoFnUnexportedExtraMethod) unexportedFunction() {
}

// Examples of incorrect DoFn signatures.
// Embedding good DoFns avoids repetitive ProcessElement signatures when desired.

type BadDoFnHasRTracker struct {
	*GoodDoFn
}

func (fn *BadDoFnHasRTracker) ProcessElement(*RTrackerT, int) int {
	return 0
}

// Examples of emit parameter mismatches.

type BadDoFnNoEmitsStartBundle struct {
	*GoodDoFnEmits
}

func (fn *BadDoFnNoEmitsStartBundle) StartBundle() {
}

type BadDoFnMissingEmitsStartBundle struct {
	*GoodDoFnEmits
}

func (fn *BadDoFnMissingEmitsStartBundle) StartBundle(func(int)) {
}

type BadDoFnMismatchedEmitsStartBundle struct {
	*GoodDoFnEmits
}

func (fn *BadDoFnMismatchedEmitsStartBundle) StartBundle(func(int), func(int)) {
}

type BadDoFnNoEmitsFinishBundle struct {
	*GoodDoFnEmits
}

func (fn *BadDoFnNoEmitsFinishBundle) FinishBundle() {
}

// Examples of side input mismatches.

type BadDoFnNoSideInputsStartBundle struct {
	*GoodDoFnSideInputs
}

func (fn *BadDoFnNoSideInputsStartBundle) StartBundle() {
}

type BadDoFnMissingSideInputsStartBundle struct {
	*GoodDoFnSideInputs
}

func (fn *BadDoFnMissingSideInputsStartBundle) StartBundle(func(*int) bool) {
}

type BadDoFnMismatchedSideInputsStartBundle struct {
	*GoodDoFnSideInputs
}

func (fn *BadDoFnMismatchedSideInputsStartBundle) StartBundle(func(*int) bool, int, func() func(*int)) {
}

type BadDoFnNoSideInputsFinishBundle struct {
	*GoodDoFnSideInputs
}

func (fn *BadDoFnNoSideInputsFinishBundle) FinishBundle() {
}

// Examples of incorrect Setup/Teardown methods.

type BadDoFnParamsInSetup struct {
	*GoodDoFn
}

func (*BadDoFnParamsInSetup) Setup(int) {
}

type BadDoFnParamsInTeardown struct {
	*GoodDoFn
}

func (*BadDoFnParamsInTeardown) Teardown(int) {
}

type BadDoFnReturnValuesInStartBundle struct {
	*GoodDoFn
}

func (*BadDoFnReturnValuesInStartBundle) StartBundle() int {
	return 0
}

type BadDoFnReturnValuesInFinishBundle struct {
	*GoodDoFn
}

func (*BadDoFnReturnValuesInFinishBundle) FinishBundle() int {
	return 0
}

type BadDoFnReturnValuesInSetup struct {
	*GoodDoFn
}

func (*BadDoFnReturnValuesInSetup) Setup() int {
	return 0
}

type BadDoFnReturnValuesInTeardown struct {
	*GoodDoFn
}

func (*BadDoFnReturnValuesInTeardown) Teardown() int {
	return 0
}

type BadDoFnAmbiguousMainInput struct {
	*GoodDoFn
}

// Ambiguous param #2 (string) is a main input but used as side input.
func (fn *BadDoFnAmbiguousMainInput) ProcessElement(int, string, bool) int {
	return 0
}

func (fn *BadDoFnAmbiguousMainInput) StartBundle(string, bool) {
}

func (fn *BadDoFnAmbiguousMainInput) FinishBundle(string, bool) {
}

type BadDoFnAmbiguousSideInput struct {
	*GoodDoFn
}

// Ambiguous param #2 (string) is a side input but used as main input.
func (fn *BadDoFnAmbiguousSideInput) ProcessElement(int, string, bool) int {
	return 0
}

func (fn *BadDoFnAmbiguousSideInput) StartBundle(bool) {
}

func (fn *BadDoFnAmbiguousSideInput) FinishBundle(bool) {
}

// Examples of correct SplittableDoFn signatures

type RestT struct{}
type RTrackerT struct{}

func (rt *RTrackerT) TryClaim(interface{}) bool {
	return true
}
func (rt *RTrackerT) GetError() error {
	return nil
}
func (rt *RTrackerT) TrySplit(fraction float64) (interface{}, interface{}, error) {
	return nil, nil, nil
}
func (rt *RTrackerT) GetProgress() (float64, float64) {
	return 0, 0
}
func (rt *RTrackerT) IsDone() bool {
	return true
}
func (rt *RTrackerT) GetRestriction() interface{} {
	return nil
}

type GoodSdf struct {
	*GoodDoFn
}

func (fn *GoodSdf) CreateInitialRestriction(int) RestT {
	return RestT{}
}

func (fn *GoodSdf) SplitRestriction(int, RestT) []RestT {
	return []RestT{}
}

func (fn *GoodSdf) RestrictionSize(int, RestT) float64 {
	return 0
}

func (fn *GoodSdf) CreateTracker(RestT) *RTrackerT {
	return &RTrackerT{}
}

func (fn *GoodSdf) ProcessElement(*RTrackerT, int) int {
	return 0
}

type GoodSdfKv struct {
	*GoodDoFnKv
}

func (fn *GoodSdfKv) CreateInitialRestriction(int, int) RestT {
	return RestT{}
}

func (fn *GoodSdfKv) SplitRestriction(int, int, RestT) []RestT {
	return []RestT{}
}

func (fn *GoodSdfKv) RestrictionSize(int, int, RestT) float64 {
	return 0
}

func (fn *GoodSdfKv) CreateTracker(RestT) *RTrackerT {
	return &RTrackerT{}
}

func (fn *GoodSdfKv) ProcessElement(*RTrackerT, int, int) int {
	return 0
}

// Examples of incorrect SDF signatures.
// Examples with missing methods.

type BadSdfMissingMethods struct {
	*GoodDoFn
}

func (fn *BadSdfMissingMethods) CreateInitialRestriction(int) RestT {
	return RestT{}
}

// Examples with incorrect numbers of parameters.

type BadSdfParamsCreateRest struct {
	*GoodSdf
}

func (fn *BadSdfParamsCreateRest) CreateInitialRestriction(int, int) RestT {
	return RestT{}
}

type BadSdfParamsSplitRest struct {
	*GoodSdf
}

func (fn *BadSdfParamsSplitRest) SplitRestriction(int, int, RestT) []RestT {
	return []RestT{}
}

type BadSdfParamsRestSize struct {
	*GoodSdf
}

func (fn *BadSdfParamsRestSize) RestrictionSize(int, int, RestT) float64 {
	return 0
}

type BadSdfParamsCreateTracker struct {
	*GoodSdf
}

func (fn *BadSdfParamsCreateTracker) CreateTracker(int, RestT) *RTrackerT {
	return &RTrackerT{}
}

// Examples with invalid numbers of return values.

type BadSdfReturnsCreateRest struct {
	*GoodSdf
}

func (fn *BadSdfReturnsCreateRest) CreateInitialRestriction(int) (RestT, int) {
	return RestT{}, 0
}

type BadSdfReturnsSplitRest struct {
	*GoodSdf
}

func (fn *BadSdfReturnsSplitRest) SplitRestriction(int, RestT) ([]RestT, int) {
	return []RestT{}, 0
}

type BadSdfReturnsRestSize struct {
	*GoodSdf
}

func (fn *BadSdfReturnsRestSize) RestrictionSize(int, RestT) (float64, int) {
	return 0, 0
}

type BadSdfReturnsCreateTracker struct {
	*GoodSdf
}

func (fn *BadSdfReturnsCreateTracker) CreateTracker(RestT) (*RTrackerT, int) {
	return &RTrackerT{}, 0
}

// Examples with element types inconsistent with ProcessElement.

type BadSdfElementTCreateRest struct {
	*GoodSdf
}

func (fn *BadSdfElementTCreateRest) CreateInitialRestriction(float32) RestT {
	return RestT{}
}

type BadSdfElementTSplitRest struct {
	*GoodSdf
}

func (fn *BadSdfElementTSplitRest) SplitRestriction(float32, RestT) []RestT {
	return []RestT{}
}

type BadSdfElementTRestSize struct {
	*GoodSdf
}

func (fn *BadSdfElementTRestSize) RestrictionSize(float32, RestT) float64 {
	return 0
}

// Examples with restriction type inconsistent CreateRestriction.

type BadRestT struct{}

type BadSdfRestTSplitRestParam struct {
	*GoodSdf
}

func (fn *BadSdfRestTSplitRestParam) SplitRestriction(int, BadRestT) []RestT {
	return []RestT{}
}

type BadSdfRestTSplitRestReturn struct {
	*GoodSdf
}

func (fn *BadSdfRestTSplitRestReturn) SplitRestriction(int, RestT) []BadRestT {
	return []BadRestT{}
}

type BadSdfRestTRestSize struct {
	*GoodSdf
}

func (fn *BadSdfRestTRestSize) RestrictionSize(int, BadRestT) float64 {
	return 0
}

type BadSdfRestTCreateTracker struct {
	*GoodSdf
}

func (fn *BadSdfRestTCreateTracker) CreateTracker(BadRestT) *RTrackerT {
	return &RTrackerT{}
}

// Examples of other type validation that needs to be done.

type BadSdfRestSizeReturn struct {
	*GoodSdf
}

func (fn *BadSdfRestSizeReturn) BadSdfRestSizeReturn(int, RestT) int {
	return 0
}

type BadRTrackerT struct{} // Fails to implement RTracker interface.

type BadSdfCreateTrackerReturn struct {
	*GoodSdf
}

func (fn *BadSdfCreateTrackerReturn) CreateTracker(RestT) *BadRTrackerT {
	return &BadRTrackerT{}
}

type BadSdfMissingRTracker struct {
	*GoodSdf
}

func (fn *BadSdfMissingRTracker) ProcessElement(int) int {
	return 0
}

type OtherRTrackerT struct {
	*RTrackerT
}

type BadSdfMismatchedRTracker struct {
	*GoodSdf
}

func (fn *BadSdfMismatchedRTracker) ProcessElement(*OtherRTrackerT, int) int {
	return 0
}

// Examples of correct CombineFn signatures

type MyAccum struct{}

type GoodCombineFn struct{}

func (fn *GoodCombineFn) MergeAccumulators(MyAccum, MyAccum) MyAccum {
	return MyAccum{}
}

func (fn *GoodCombineFn) CreateAccumulator() MyAccum {
	return MyAccum{}
}

func (fn *GoodCombineFn) AddInput(MyAccum, int) MyAccum {
	return MyAccum{}
}

func (fn *GoodCombineFn) ExtractOutput(MyAccum) int64 {
	return 0
}

type GoodWErrorCombineFn struct{}

func (fn *GoodWErrorCombineFn) MergeAccumulators(int, int) (int, error) {
	return 0, nil
}

type GoodWContextCombineFn struct{}

func (fn *GoodWContextCombineFn) MergeAccumulators(context.Context, MyAccum, MyAccum) MyAccum {
	return MyAccum{}
}

func (fn *GoodWContextCombineFn) CreateAccumulator(context.Context) MyAccum {
	return MyAccum{}
}

func (fn *GoodWContextCombineFn) AddInput(context.Context, MyAccum, int) MyAccum {
	return MyAccum{}
}

func (fn *GoodWContextCombineFn) ExtractOutput(context.Context, MyAccum) int64 {
	return 0
}

type GoodCombineFnUnexportedExtraMethod struct {
	*GoodCombineFn
}

func (fn *GoodCombineFnUnexportedExtraMethod) unexportedExtraMethod(context.Context, string) string {
	return ""
}

// Examples of incorrect CombineFn signatures.
// Embedding *GoodCombineFn avoids repetitive MergeAccumulators signatures when desired.
// The immediately following examples are relating to accumulator mismatches.

type BadCombineFnNoMergeAccumulators struct{}

func (fn *BadCombineFnNoMergeAccumulators) CreateAccumulator() string { return "" }

type BadCombineFnNonBinaryMergeAccumulators struct {
	*GoodCombineFn
}

func (fn *BadCombineFnNonBinaryMergeAccumulators) MergeAccumulators(int, string) int {
	return 0
}

type BadCombineFnMisMatchedCreateAccumulator struct {
	*GoodCombineFn
}

func (fn *BadCombineFnMisMatchedCreateAccumulator) CreateAccumulator() string {
	return ""
}

type BadCombineFnMisMatchedAddInputIn struct {
	*GoodCombineFn
}

func (fn *BadCombineFnMisMatchedAddInputIn) AddInput(string, int) MyAccum {
	return MyAccum{}
}

type BadCombineFnMisMatchedAddInputOut struct {
	*GoodCombineFn
}

func (fn *BadCombineFnMisMatchedAddInputOut) AddInput(MyAccum, int) string {
	return ""
}

type BadCombineFnMisMatchedAddInputBoth struct {
	*GoodCombineFn
}

func (fn *BadCombineFnMisMatchedAddInputBoth) AddInput(string, int) string {
	return ""
}

type BadCombineFnMisMatchedExtractOutput struct {
	*GoodCombineFn
}

func (fn *BadCombineFnMisMatchedExtractOutput) ExtractOutput(string) int {
	return 0
}

// Examples of incorrect CreateAccumulator signatures

type BadCombineFnInvalidCreateAccumulator1 struct {
	*GoodCombineFn
}

func (fn *BadCombineFnInvalidCreateAccumulator1) CreateAccumulator(context.Context, string) int {
	return 0
}

type BadCombineFnInvalidCreateAccumulator2 struct {
	*GoodCombineFn
}

func (fn *BadCombineFnInvalidCreateAccumulator2) CreateAccumulator(string) int {
	return 0
}

type BadCombineFnInvalidCreateAccumulator3 struct {
	*GoodCombineFn
}

func (fn *BadCombineFnInvalidCreateAccumulator3) CreateAccumulator() (MyAccum, string) {
	return MyAccum{}, ""
}

type BadCombineFnInvalidCreateAccumulator4 struct {
	*GoodCombineFn
}

func (fn *BadCombineFnInvalidCreateAccumulator4) CreateAccumulator() (string, MyAccum) {
	return "", MyAccum{}
}

// Examples of incorrect AddInput signatures

type BadCombineFnInvalidAddInput1 struct {
	*GoodCombineFn
}

func (fn *BadCombineFnInvalidAddInput1) AddInput(context.Context, string) int {
	return 0
}

type BadCombineFnInvalidAddInput2 struct {
	*GoodCombineFn
}

func (fn *BadCombineFnInvalidAddInput2) AddInput(string) int {
	return 0
}

type BadCombineFnInvalidAddInput3 struct {
	*GoodCombineFn
}

func (fn *BadCombineFnInvalidAddInput3) AddInput(context.Context, string, string, string) int {
	return 0
}

type BadCombineFnInvalidAddInput4 struct {
	*GoodCombineFn
}

func (fn *BadCombineFnInvalidAddInput4) AddInput(MyAccum, string) (int, int, int) {
	return 0, 0, 0
}

// Examples of incorrect ExtractOutput signatures

type BadCombineFnInvalidExtractOutput1 struct {
	*GoodCombineFn
}

func (fn *BadCombineFnInvalidExtractOutput1) ExtractOutput(MyAccum, string) (int, int, int) {
	return 0, 0, 0
}

type BadCombineFnInvalidExtractOutput2 struct {
	*GoodCombineFn
}

func (fn *BadCombineFnInvalidExtractOutput2) ExtractOutput() (int, int, int) {
	return 0, 0, 0
}

type BadCombineFnInvalidExtractOutput3 struct {
	*GoodCombineFn
}

func (fn *BadCombineFnInvalidExtractOutput3) ExtractOutput(context.Context, MyAccum, int) int {
	return 0
}

// Other CombineFn Errors

type BadCombineFnExtraExportedMethod struct {
	*GoodCombineFn
}

func (fn *BadCombineFnExtraExportedMethod) ExtraMethod(string) int {
	return 0
}
