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
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/rtrackers/offsetrange"
	"math/rand"
	"time"
)

// Step creates a synthetic step transform that receives KV<[]byte, []byte>
// elements from other synthetic transforms, and outputs KV<[]byte, []byte>
// elements based on its inputs.
//
// This function accepts a StepConfig to configure the behavior of the synthetic
// step, including whether that step is implemented as a splittable or
// non-splittable DoFn.
//
// StepConfigs are recommended to be created via the DefaultStepConfig and
// modified before being passed to this method. Example:
//
//    cfg := synthetic.DefaultStepConfig()
//    cfg.OutputPerInput = 1000
//    cfg.Splittable = true
//    cfg.InitialSplits = 2
//    step := synthetic.Step(s, cfg, input)
func Step(s beam.Scope, cfg StepConfig, col beam.PCollection) beam.PCollection {
	s = s.Scope("synthetic.Step")
	if cfg.Splittable {
		return beam.ParDo(s, &sdfStepFn{cfg: cfg}, col)
	} else {
		return beam.ParDo(s, &stepFn{cfg: cfg}, col)
	}
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
	if fn.cfg.FilterRatio > 0 && fn.rng.Float64() < fn.cfg.FilterRatio {
		return
	}
	for i := 0; i < fn.cfg.OutputPerInput; i++ {
		emit(key, val)
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
func (fn *sdfStepFn) CreateInitialRestriction(key, val []byte) offsetrange.Restriction {
	return offsetrange.Restriction{
		Start: 0,
		End:   int64(fn.cfg.OutputPerInput),
	}
}

// SplitRestriction splits restrictions equally according to the number of
// initial splits specified in StepConfig. Each restriction output by this
// method will contain at least one element, so the number of splits will not
// exceed the number of elements.
func (fn *sdfStepFn) SplitRestriction(key, val []byte, rest offsetrange.Restriction) (splits []offsetrange.Restriction) {
	if fn.cfg.InitialSplits <= 1 {
		// Don't split, just return original restriction.
		return append(splits, rest)
	}

	// TODO(BEAM-9978) Move this implementation of the offset range restriction
	// splitting to the restriction itself, and add testing.
	num := int64(fn.cfg.InitialSplits)
	offset := rest.Start
	size := rest.End - rest.Start
	for i := int64(0); i < num; i++ {
		split := offsetrange.Restriction{
			Start: offset + (i * size / num),
			End:   offset + ((i + 1) * size / num),
		}
		// Skip restrictions that end up empty.
		if split.End-split.Start <= 0 {
			continue
		}
		splits = append(splits, split)
	}
	return splits
}

// RestrictionSize outputs the size of the restriction as the number of elements
// that restriction will output.
func (fn *sdfStepFn) RestrictionSize(key, val []byte, rest offsetrange.Restriction) float64 {
	// TODO(BEAM-9978) Move this size implementation to the offset range restriction itself.
	return float64(rest.End - rest.Start)
}

// CreateTracker creates an offset range restriction tracker for the
// restriction.
func (fn *sdfStepFn) CreateTracker(rest offsetrange.Restriction) *offsetrange.Tracker {
	return offsetrange.NewTracker(rest)
}

// Setup sets up the random number generator.
func (fn *sdfStepFn) Setup() {
	fn.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
}

// ProcessElement takes an input and either filters it or produces a number of
// outputs identical to that input based on the restriction size.
func (fn *sdfStepFn) ProcessElement(rt *offsetrange.Tracker, key, val []byte, emit func([]byte, []byte)) {
	if fn.cfg.FilterRatio > 0 && fn.rng.Float64() < fn.cfg.FilterRatio {
		return
	}
	for i := rt.Rest.Start; rt.TryClaim(i) == true; i++ {
		emit(key, val)
	}
}

// DefaultSourceConfig creates a SourceConfig with intended defaults for its
// fields. SourceConfigs should be initialized with this method.
func DefaultStepConfig() StepConfig {
	return StepConfig{
		OutputPerInput: 1,     // Defaults shouldn't drop elements, so at least 1.
		FilterRatio:    0.0,   // Defaults shouldn't drop elements, so don't filter.
		Splittable:     false, // Default to non-splittable, SDFs are situational.
		InitialSplits:  1,     // Defaults to 1, i.e. no initial splitting.
	}
}

// StepConfig is a struct containing all the configuration options for a
// synthetic step.
type StepConfig struct {
	// OutputPerInput is the number of outputs to emit per input received. Each
	// output is identical to the original input. A value of 0 drops each input.
	OutputPerInput int

	// FilterRatio indicates the random chance that an input will be filtered
	// out, meaning that no outputs will get emitted for it. For example, a
	// FilterRatio of 0.25 means that 25% of inputs will get filtered out.
	FilterRatio float64

	// Splittable indicates whether the step should use the splittable DoFn or
	// non-splittable DoFn implementation. Splittable steps will split the
	// number of OutputPerInput into restrictions, so it is most useful for
	// steps with a high OutputPerInput.
	Splittable bool

	// InitialSplits is only applicable if Splittable is set to true, and
	// determines the number of initial splits to perform in the step's
	// SplitRestriction method. Note that in some edge cases, the number of
	// splits performed might differ from this config value. Each restriction
	// will always have one element in it, and at least one restriction will
	// always be output, so the number of splits will be in the range of [1, N]
	// where N is the size of the original restriction.
	InitialSplits int
}
