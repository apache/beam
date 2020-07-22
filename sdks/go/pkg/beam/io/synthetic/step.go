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

package synthetic

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/core/sdf"
	"math/rand"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/rtrackers/offsetrange"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*stepFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*sdfStepFn)(nil)).Elem())
}

// Step creates a synthetic step transform that receives KV<[]byte, []byte>
// elements from other synthetic transforms, and outputs KV<[]byte, []byte>
// elements based on its inputs.
//
// This function accepts a StepConfig to configure the behavior of the synthetic
// step, including whether that step is implemented as a splittable or
// non-splittable DoFn.
//
// The recommended way to create StepConfigs is via the StepConfigBuilder.
// Usage example:
//
//    cfg := synthetic.DefaultStepConfig().OutputPerInput(10).FilterRatio(0.5).Build()
//    step := synthetic.Step(s, cfg, input)
func Step(s beam.Scope, cfg StepConfig, col beam.PCollection) beam.PCollection {
	s = s.Scope("synthetic.Step")
	if cfg.Splittable {
		return beam.ParDo(s, &sdfStepFn{cfg: cfg}, col)
	}
	return beam.ParDo(s, &stepFn{cfg: cfg}, col)
}

// stepFn is a DoFn implementing behavior for synthetic steps. For usage
// information, see synthetic.Step.
//
// The stepFn is expected to be initialized with a cfg and will follow that
// config to determine its behavior when emitting elements.
type stepFn struct {
	cfg StepConfig
	rng randWrapper
}

// Setup sets up the random number generator.
func (fn *stepFn) Setup() {
	fn.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
}

// ProcessElement takes an input and either filters it or produces a number of
// outputs identical to that input based on the outputs per input configuration
// in StepConfig.
func (fn *stepFn) ProcessElement(key, val []byte, emit func([]byte, []byte)) {
	filtered := fn.cfg.FilterRatio > 0 && fn.rng.Float64() < fn.cfg.FilterRatio

	for i := 0; i < fn.cfg.OutputPerInput; i++ {
		if !filtered {
			emit(key, val)
		}
	}
}

// sdfStepFn is a splittable DoFn implementing behavior for synthetic steps.
// For usage information, see synthetic.Step.
//
// The sdfStepFn is expected to be initialized with a cfg and will follow
// that config to determine its behavior when splitting and emitting elements.
type sdfStepFn struct {
	cfg StepConfig
	rng randWrapper
}

// CreateInitialRestriction creates an offset range restriction representing
// the number of elements to emit for this received element, as specified by
// the output per input configuration in StepConfig.
func (fn *sdfStepFn) CreateInitialRestriction(_, _ []byte) offsetrange.Restriction {
	return offsetrange.Restriction{
		Start: 0,
		End:   int64(fn.cfg.OutputPerInput),
	}
}

// SplitRestriction splits restrictions equally according to the number of
// initial splits specified in StepConfig. Each restriction output by this
// method will contain at least one element, so the number of splits will not
// exceed the number of elements.
func (fn *sdfStepFn) SplitRestriction(_, _ []byte, rest offsetrange.Restriction) (splits []offsetrange.Restriction) {
	return rest.EvenSplits(int64(fn.cfg.InitialSplits))
}

// RestrictionSize outputs the size of the restriction as the number of elements
// that restriction will output.
func (fn *sdfStepFn) RestrictionSize(_, _ []byte, rest offsetrange.Restriction) float64 {
	return rest.Size()
}

// CreateTracker creates an offset range restriction tracker for the
// restriction.
func (fn *sdfStepFn) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
}

// Setup sets up the random number generator.
func (fn *sdfStepFn) Setup() {
	fn.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
}

// ProcessElement takes an input and either filters it or produces a number of
// outputs identical to that input based on the restriction size.
func (fn *sdfStepFn) ProcessElement(rt *sdf.LockRTracker, key, val []byte, emit func([]byte, []byte)) {
	filtered := fn.cfg.FilterRatio > 0 && fn.rng.Float64() < fn.cfg.FilterRatio

	for i := rt.GetRestriction().(offsetrange.Restriction).Start; rt.TryClaim(i) == true; i++ {
		if !filtered {
			emit(key, val)
		}
	}
}

// StepConfigBuilder is used to initialize StepConfigs. See StepConfigBuilder's
// methods for descriptions of the fields in a StepConfig and how they can be
// set. The intended approach for using this builder is to begin by calling the
// DefaultStepConfig function, followed by calling setters, followed by calling
// Build.
//
// Usage example:
//
//    cfg := synthetic.DefaultStepConfig().OutputPerInput(10).FilterRatio(0.5).Build()
type StepConfigBuilder struct {
	cfg StepConfig
}

// DefaultStepConfig creates a StepConfig with intended defaults for the
// StepConfig fields. This function is the intended starting point for
// initializing a StepConfig and should always be used to create
// StepConfigBuilders.
//
// To see descriptions of the various StepConfig fields and their defaults, see
// the methods to StepConfigBuilder.
func DefaultStepConfig() *StepConfigBuilder {
	return &StepConfigBuilder{
		cfg: StepConfig{
			OutputPerInput: 1,     // Defaults shouldn't drop elements, so at least 1.
			FilterRatio:    0.0,   // Defaults shouldn't drop elements, so don't filter.
			Splittable:     false, // Default to non-splittable, SDFs are situational.
			InitialSplits:  1,     // Defaults to 1, i.e. no initial splitting.
		},
	}
}

// OutputPerInput is the number of outputs to emit per input received. Each
// output is identical to the original input. A value of 0 drops all inputs and
// produces no output.
//
// Valid values are in the range of [0, ...] and the default value is 1. Values
// below 0 are invalid as they have no logical meaning for this field.
func (b *StepConfigBuilder) OutputPerInput(val int) *StepConfigBuilder {
	b.cfg.OutputPerInput = val
	return b
}

// FilterRatio indicates the random chance that an input will be filtered
// out, meaning that no outputs will get emitted for it. For example, a
// FilterRatio of 0.25 means that 25% of inputs will be filtered out, a
// FilterRatio of 0 means no elements are filtered, and a FilterRatio of 1.0
// means every element is filtered.
//
// In a non-splittable step, this is performed on each input element, meaning
// all outputs for that element would be filtered. In a splittable step, this is
// performed on each input restriction instead of the entire element, meaning
// that some outputs for an element may be filtered and others kept.
//
// Note that even when elements are filtered out, the work associated with
// processing those elements is still performed, which differs from setting an
// OutputPerInput of 0. Also note that if a
//
// Valid values are in the range if [0.0, 1.0], and the default value is 0. In
// order to avoid precision errors, invalid values do not cause errors. Instead,
// values below 0 are functionally equivalent to 0, and values above 1 are
// functionally equivalent to 1.
func (b *StepConfigBuilder) FilterRatio(val float64) *StepConfigBuilder {
	b.cfg.FilterRatio = val
	return b
}

// Splittable indicates whether the step should use the splittable DoFn or
// non-splittable DoFn implementation.
//
// Splittable steps will split along restrictions representing the number of
// OutputPerInput for each element, so it is most useful for steps with a high
// OutputPerInput. Conversely, if OutputPerInput is 1, then there is no way to
// split restrictions further, so making the step splittable will do nothing.
func (b *StepConfigBuilder) Splittable(val bool) *StepConfigBuilder {
	b.cfg.Splittable = val
	return b
}

// InitialSplits is only applicable if Splittable is set to true, and determines
// the number of initial splits to perform in the step's SplitRestriction
// method. Restrictions in synthetic steps represent the number of elements to
// emit for each input element, as defined by the OutputPerInput config field,
// and this split is performed evenly across that number of elements.
//
// Each resulting restriction will have at least 1 element in it, and each
// element being emitted will be contained in exactly one restriction. That
// means that if the desired number of splits is greater than the OutputPerInput
// N, then N initial restrictions will be created, each containing 1 element.
//
// Valid values are in the range of [1, ...] and the default value is 1. Values
// of 0 (and below) are invalid as they would result in dropping elements that
// are expected to be emitted.
func (b *StepConfigBuilder) InitialSplits(val int) *StepConfigBuilder {
	b.cfg.InitialSplits = val
	return b
}

// Build constructs the StepConfig initialized by this builder. It also performs
// error checking on the fields, and panics if any have been set to invalid
// values.
func (b *StepConfigBuilder) Build() StepConfig {
	if b.cfg.InitialSplits <= 0 {
		panic(fmt.Sprintf("StepConfig.InitialSplits must be >= 1. Got: %v", b.cfg.InitialSplits))
	}
	if b.cfg.OutputPerInput < 0 {
		panic(fmt.Sprintf("StepConfig.OutputPerInput cannot be negative. Got: %v", b.cfg.OutputPerInput))
	}
	return b.cfg
}

// StepConfig is a struct containing all the configuration options for a
// synthetic step. It should be created via a StepConfigBuilder, not by directly
// initializing it (the fields are public to allow encoding).
type StepConfig struct {
	OutputPerInput int
	FilterRatio    float64
	Splittable     bool
	InitialSplits  int
}
