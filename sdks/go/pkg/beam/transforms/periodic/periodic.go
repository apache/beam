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
	register.Function2x0(sequenceToImpulse)
	register.Emitter1[[]byte]()
	beam.RegisterType(reflect.TypeOf(SequenceDefinition{}))
}

// SequenceDefinition holds the configuration for generating a sequence of
// timestamped elements at an interval.
type SequenceDefinition struct {
	Interval time.Duration

	// Start is the number of milliseconds since the Unix epoch.
	Start int64

	// End is the number of milliseconds since the Unix epoch.
	End int64
}

// NewSequenceDefinition creates a new [SequenceDefinition] from a start and
// end [time.Time] along with its interval [time.Duration].
func NewSequenceDefinition(start, end time.Time, interval time.Duration) SequenceDefinition {
	return SequenceDefinition{
		Start:    start.UnixMilli(),
		End:      end.UnixMilli(),
		Interval: interval,
	}
}

func CalculateByteSizeOfSequence(now time.Time, sd SequenceDefinition, rest offsetrange.Restriction) int64 {
	// Find the # of outputs expected for overlap of  and [-inf, now)
	nowIndex := int64(now.Sub(mtime.Time(sd.Start).ToTime()) / sd.Interval)
	if nowIndex < rest.Start {
		return 0
	}
	return 8 * (min(rest.End, nowIndex) - rest.Start)
}

type sequenceGenDoFn struct{}

func (fn *sequenceGenDoFn) CreateInitialRestriction(sd SequenceDefinition) offsetrange.Restriction {
	totalOutputs := mtime.Time(sd.End).ToTime().Sub(mtime.Time(sd.Start).ToTime()) / sd.Interval
	return offsetrange.Restriction{
		Start: int64(0),
		End:   int64(totalOutputs),
	}
}

func (fn *sequenceGenDoFn) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
}

func (fn *sequenceGenDoFn) RestrictionSize(sd SequenceDefinition, rest offsetrange.Restriction) float64 {
	return float64(CalculateByteSizeOfSequence(time.Now(), sd, rest))
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
	currentOutputTimestamp := mtime.Time(sd.Start).ToTime().Add(sd.Interval * time.Duration(currentOutputIndex))
	currentTime := time.Now()
	we.UpdateWatermark(currentOutputTimestamp)
	for currentOutputTimestamp.Before(currentTime) {
		if rt.TryClaim(currentOutputIndex) {
			emit(mtime.FromTime(currentOutputTimestamp), currentOutputIndex)
			currentOutputIndex++
			currentOutputTimestamp = mtime.Time(sd.Start).ToTime().Add(sd.Interval * time.Duration(currentOutputIndex))
			currentTime = time.Now()
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

// Impulse is a PTransform which generates a sequence of timestamped
// elements at fixed runtime intervals. If applyWindow is specified, each
// element will be assigned to its own fixed window of interval size.
//
// The transform behaves the same as [Sequence] transform, but can be
// used as the first transform in a pipeline.
//
// The following applies to the arguments.
//   - if start is a zero value [time.Time], start is set to the current time
//   - if start is after end, start is set to end
//   - start and end are normalized with [mtime.Normalize]
//   - if interval <= 0 or interval > end.Sub(start), interval is set to end.Sub(start)
//
// The PCollection<[]byte> generated by Impulse is unbounded.
func Impulse(s beam.Scope, start, end time.Time, interval time.Duration, applyWindow bool) beam.PCollection {
	if start.IsZero() {
		start = time.Now()
	}
	if start.After(end) {
		start = end
	}
	start = mtime.Normalize(mtime.FromTime(start)).ToTime()
	end = mtime.Normalize(mtime.FromTime(end)).ToTime()
	if interval <= 0 || interval > end.Sub(start) {
		interval = end.Sub(start)
	}

	return genImpulse(s.Scope("periodic.Impulse"), start, end, interval, applyWindow, &sequenceGenDoFn{})
}

func genImpulse(s beam.Scope, start, end time.Time, interval time.Duration, applyWindow bool, fn *sequenceGenDoFn) beam.PCollection {
	sd := SequenceDefinition{Interval: interval, Start: start.UnixMilli(), End: end.UnixMilli()}
	imp := beam.Create(s.Scope("ImpulseElement"), sd)
	seq := genSequence(s, imp, fn)
	imps := beam.ParDo(s, sequenceToImpulse, seq)
	if applyWindow {
		return beam.WindowInto(s.Scope("ApplyWindowing"),
			window.NewFixedWindows(interval), imps)
	}
	return imps
}

func sequenceToImpulse(_ int64, emit func([]byte)) {
	emit([]byte{})
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
// The PCollection<int64> generated by Sequence is unbounded.
func Sequence(s beam.Scope, col beam.PCollection) beam.PCollection {
	return genSequence(s.Scope("periodic.Sequence"), col, &sequenceGenDoFn{})
}

func genSequence(s beam.Scope, col beam.PCollection, fn *sequenceGenDoFn) beam.PCollection {
	return beam.ParDo(s.Scope("GenSequence"), fn, col)
}
