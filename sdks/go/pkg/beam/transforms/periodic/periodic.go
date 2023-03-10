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

// Package periodic contains transformations for generating periodic sequences.
package periodic

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn5x2[context.Context, *sdf.ManualWatermarkEstimator, *sdf.LockRTracker, SequenceDefinition,
		func(beam.EventTime, int64),
		sdf.ProcessContinuation, error](&sequenceGenDoFn{})
	register.Emitter2[beam.EventTime, int64]()
	beam.RegisterType(reflect.TypeOf(SequenceDefinition{}))
}

// SequenceDefinition holds the configuration for generating a sequence of
// timestamped elements at an interval.
type SequenceDefinition struct {
	Interval time.Duration
	Start    time.Time
	End      time.Time
}

type sequenceGenDoFn struct {
	now func() time.Time
}

func (fn *sequenceGenDoFn) Setup() {
	if fn.now == nil {
		fn.now = time.Now
	}
}

func (fn *sequenceGenDoFn) CreateInitialRestriction(sd SequenceDefinition) offsetrange.Restriction {
	totalOutputs := math.Ceil(float64(sd.End.Sub(sd.Start) / sd.Interval))
	return offsetrange.Restriction{
		Start: int64(0),
		End:   int64(totalOutputs),
	}
}

func (fn *sequenceGenDoFn) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
}

func (fn *sequenceGenDoFn) RestrictionSize(_ SequenceDefinition, rest offsetrange.Restriction) float64 {
	return rest.Size()
}

func (fn *sequenceGenDoFn) SplitRestriction(_ SequenceDefinition, rest offsetrange.Restriction) []offsetrange.Restriction {
	return []offsetrange.Restriction{rest}
}

// TruncateRestriction immediately truncates the entire restrication.
func (fn *sequenceGenDoFn) TruncateRestriction(_ *sdf.LockRTracker, _ SequenceDefinition) offsetrange.Restriction {
	return offsetrange.Restriction{}
}

func (fn *sequenceGenDoFn) CreateWatermarkEstimator() *sdf.ManualWatermarkEstimator {
	return &sdf.ManualWatermarkEstimator{}
}

func (fn *sequenceGenDoFn) ProcessElement(ctx context.Context, we *sdf.ManualWatermarkEstimator, rt *sdf.LockRTracker, sd SequenceDefinition, emit func(beam.EventTime, int64)) (sdf.ProcessContinuation, error) {
	currentOutputIndex := rt.GetRestriction().(offsetrange.Restriction).Start
	currentOutputTimestamp := sd.Start.Add(sd.Interval * time.Duration(currentOutputIndex))
	currentTime := fn.now()
	we.UpdateWatermark(currentOutputTimestamp)
	for currentOutputTimestamp.Before(currentTime) {
		if rt.TryClaim(currentOutputIndex) {
			emit(mtime.FromTime(currentOutputTimestamp), currentOutputTimestamp.UnixMilli())
			currentOutputIndex += 1
			currentOutputTimestamp = sd.Start.Add(sd.Interval * time.Duration(currentOutputIndex))
			currentTime = fn.now()
			we.UpdateWatermark(currentOutputTimestamp)
		} else if err := rt.GetError(); err != nil || rt.IsDone() {
			// Stop processing on error or completion
			return sdf.StopProcessing(), rt.GetError()
		} else {
			return sdf.ResumeProcessingIn(sd.Interval), nil
		}
	}

	return sdf.ResumeProcessingIn(time.Until(currentOutputTimestamp)), nil
}

type impulseConfig struct {
	ApplyWindow bool

	now func() time.Time
}

type impulseOption func(*impulseConfig) error

// ImpulseOption is a function that configures an [Impulse] transform.
type ImpulseOption = impulseOption

// WithApplyWindow configures the [Impulse] transform to apply a fixed window
// transform to the output PCollection.
func WithApplyWindow() ImpulseOption {
	return func(o *impulseConfig) error {
		o.ApplyWindow = true
		return nil
	}
}

func withNowFunc(now func() time.Time) ImpulseOption {
	return func(o *impulseConfig) error {
		o.now = now
		return nil
	}
}

// Impulse is a PTransform which generates a sequence of timestamped
// elements at fixed runtime intervals. If [WithApplyWindow] is specified, each
// element will be assigned to its own fixed window of interval size.
//
// The transform behaves the same as [Sequence] transform, but can be
// used as the first transform in a pipeline.
//
// The following applies to the arguments.
//   - if interval <= 0, interval is set to [math.MaxInt64]
//   - if start is a zero value [time.Time], start is set to the current time
//   - if start is after end, start is set to end
//
// The PCollection generated by Impulse is unbounded and the output elements
// are the [time.UnixMilli] int64 values of the output timestamp.
func Impulse(s beam.Scope, start, end time.Time, interval time.Duration, opts ...ImpulseOption) beam.PCollection {
	if interval <= 0 {
		interval = math.MaxInt64
	}
	if start.IsZero() {
		start = time.Now()
	}
	if start.After(end) {
		start = end
	}

	conf := impulseConfig{}

	for _, opt := range opts {
		if err := opt(&conf); err != nil {
			panic(fmt.Errorf("periodic.Impulse: invalid option: %v", err))
		}
	}

	return genImpulse(s.Scope("periodic.Impulse"), start, end, interval, conf, &sequenceGenDoFn{now: conf.now})
}

func genImpulse(s beam.Scope, start, end time.Time, interval time.Duration, conf impulseConfig, fn *sequenceGenDoFn) beam.PCollection {
	sd := SequenceDefinition{Interval: interval, Start: start, End: end}
	imp := beam.Create(s.Scope("ImpulseElement"), sd)
	col := genSequence(s, imp, fn)
	if conf.ApplyWindow {
		return beam.WindowInto(s.Scope("ApplyWindowing"),
			window.NewFixedWindows(interval), col)
	}
	return col
}

// Sequence is a PTransform which generates a sequence of timestamped
// elements at fixed runtime intervals.
//
// The transform assigns each element a timestamp and will only output an
// element once the worker clock reach the output timestamp. Sequence is not
// able to guarantee that elements are output at the their exact timestamp, but
// it guarantees that elements will not be output prior to runtime timestamp.
//
// The transform will not output elements prior to the start time.
//
// Sequence receives [SequenceDefinition] elements and for each input element
// received, it will start generating output elements in the following pattern:
//
//   - if element timestamp is less than current runtime then output element.
//   - if element timestamp is greater than current runtime, wait until next
//     element timestamp.
//
// The PCollection generated by Sequence is unbounded and the output elements
// are the [time.UnixMilli] int64 values of the output timestamp.
func Sequence(s beam.Scope, col beam.PCollection) beam.PCollection {
	return genSequence(s.Scope("periodic.Sequence"), col, &sequenceGenDoFn{})
}

func genSequence(s beam.Scope, col beam.PCollection, fn *sequenceGenDoFn) beam.PCollection {
	return beam.ParDo(s.Scope("GenSequence"), fn, col)
}
