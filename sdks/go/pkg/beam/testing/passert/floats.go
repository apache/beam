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

package passert

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn2x1[[]byte, func(*beam.T) bool, error]((*boundsFn)(nil))
	register.DoFn3x1[[]byte, func(*beam.T) bool, func(*beam.T) bool, error]((*thresholdFn)(nil))
	register.Emitter1[beam.T]()
	register.Iter1[beam.T]()
}

// EqualsFloat calls into TryEqualsFloat, checkong that two PCollections of non-complex
// numeric types are equal, with each element being within a provided threshold of an
// expected value. Panics if TryEqualsFloat returns an error.
func EqualsFloat(s beam.Scope, observed, expected beam.PCollection, threshold float64) {
	if err := TryEqualsFloat(s, observed, expected, threshold); err != nil {
		panic(fmt.Sprintf("TryEqualsFloat failed: %v", err))
	}
}

// TryEqualsFloat checks that two PCollections of floats are equal, with each element
// being within a specified threshold of its corresponding element. Both PCollections
// are loaded into memory, sorted, and compared element by element. Returns an error if
// the PCollection types are complex or non-numeric.
func TryEqualsFloat(s beam.Scope, observed, expected beam.PCollection, threshold float64) error {
	errorStrings := []string{}
	observedT := beam.ValidateNonCompositeType(observed)
	if obsErr := validateNonComplexNumber(observedT.Type()); obsErr != nil {
		errorStrings = append(errorStrings, fmt.Sprintf("observed PCollection has incompatible type: %v", obsErr))
	}
	expectedT := beam.ValidateNonCompositeType(expected)
	validateNonComplexNumber(expectedT.Type())
	if expErr := validateNonComplexNumber(expectedT.Type()); expErr != nil {
		errorStrings = append(errorStrings, fmt.Sprintf("expected PCollection has incompatible type: %v", expErr))
	}
	if len(errorStrings) != 0 {
		return errors.New(strings.Join(errorStrings, "\n"))
	}
	s = s.Scope(fmt.Sprintf("passert.EqualsFloat[%v]", threshold))
	beam.ParDo0(s, &thresholdFn{Threshold: threshold}, beam.Impulse(s), beam.SideInput{Input: observed}, beam.SideInput{Input: expected})
	return nil
}

type thresholdFn struct {
	Threshold float64
}

func (f *thresholdFn) ProcessElement(_ []byte, observed, expected func(*beam.T) bool) error {
	var observedValues, expectedValues []float64
	var observedInput, expectedInput beam.T
	for observed(&observedInput) {
		val := toFloat(observedInput)
		observedValues = append(observedValues, val)
	}
	for expected(&expectedInput) {
		val := toFloat(expectedInput)
		expectedValues = append(expectedValues, val)
	}
	if len(observedValues) != len(expectedValues) {
		return errors.Errorf("PCollections of different lengths, got %v expected %v", len(observedValues), len(expectedValues))
	}
	sort.Float64s(observedValues)
	sort.Float64s(expectedValues)
	var tooLow, tooHigh []string
	for i := 0; i < len(observedValues); i++ {
		delta := observedValues[i] - expectedValues[i]
		if delta > f.Threshold {
			tooHigh = append(tooHigh, fmt.Sprintf("%v > %v,", observedValues[i], expectedValues[i]))
		} else if delta < f.Threshold*-1 {
			tooLow = append(tooLow, fmt.Sprintf("%v < %v,", observedValues[i], expectedValues[i]))
		}
	}
	if len(tooLow)+len(tooHigh) == 0 {
		return nil
	}
	errorStrings := []string{}
	if len(tooLow) != 0 {
		errorStrings = append(errorStrings, fmt.Sprintf("values below expected: %v", tooLow))
	}
	if len(tooHigh) != 0 {
		errorStrings = append(errorStrings, fmt.Sprintf("values above expected: %v", tooHigh))
	}
	return errors.New(strings.Join(errorStrings, "\n"))
}

// AllWithinBounds checks that a PCollection of numeric types is within the bounds
// [lo, high]. Checks for case where bounds are flipped and swaps them so the bounds
// passed to the doFn are always lo <= hi.
func AllWithinBounds(s beam.Scope, col beam.PCollection, lo, hi float64) {
	t := beam.ValidateNonCompositeType(col)
	validateNonComplexNumber(t.Type())
	if lo > hi {
		lo, hi = hi, lo
	}
	s = s.Scope(fmt.Sprintf("passert.AllWithinBounds([%v, %v])", lo, hi))
	beam.ParDo0(s, &boundsFn{Lo: lo, Hi: hi}, beam.Impulse(s), beam.SideInput{Input: col})
}

type boundsFn struct {
	Lo, Hi float64
}

func (f *boundsFn) ProcessElement(_ []byte, col func(*beam.T) bool) error {
	var tooLow, tooHigh []float64
	var input beam.T
	for col(&input) {
		val := toFloat(input)
		if val < f.Lo {
			tooLow = append(tooLow, val)
		} else if val > f.Hi {
			tooHigh = append(tooHigh, val)
		}
	}
	if len(tooLow)+len(tooHigh) == 0 {
		return nil
	}
	errorStrings := []string{}
	if len(tooLow) != 0 {
		sort.Float64s(tooLow)
		errorStrings = append(errorStrings, fmt.Sprintf("values below minimum value %v: %v", f.Lo, tooLow))
	}
	if len(tooHigh) != 0 {
		sort.Float64s(tooHigh)
		errorStrings = append(errorStrings, fmt.Sprintf("values above maximum value %v: %v", f.Hi, tooHigh))
	}
	return errors.New(strings.Join(errorStrings, "\n"))
}

func toFloat(input beam.T) float64 {
	return reflect.ValueOf(input.(any)).Convert(reflectx.Float64).Interface().(float64)
}

func validateNonComplexNumber(t reflect.Type) error {
	if !reflectx.IsNumber(t) || reflectx.IsComplex(t) {
		return errors.Errorf("type must be a non-complex number: %v", t)
	}
	return nil
}
