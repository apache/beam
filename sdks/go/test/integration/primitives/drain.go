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

package primitives

import (
	"context"
	"flag"
	"math"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

var (
	output = flag.String("output", "", "output pubsub topic to write to.")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*TruncateFn)(nil)).Elem())
}

type RangeEstimator struct {
	End int64
}

func (r *RangeEstimator) Estimate() int64 {
	return r.End
}

func (r *RangeEstimator) SetEstimate(estimate int64) {
	r.End = estimate
}

type TruncateFn struct {
	Estimator RangeEstimator
}

// CreateInitialRestriction creates an initial restriction
func (fn *TruncateFn) CreateInitialRestriction(b []byte) offsetrange.Restriction {
	return offsetrange.Restriction{
		Start: int64(1),
		End:   int64(math.MaxInt64),
	}
}

// CreateTracker wraps the fiven restriction into a LockRTracker type.
func (fn *TruncateFn) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	// return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
	fn.Estimator = RangeEstimator{int64(10)}
	tracker, err := offsetrange.NewGrowableTracker(rest, &fn.Estimator)
	if err != nil {
		panic(err)
	}
	return sdf.NewLockRTracker(tracker)
}

// RestrictionSize returns the size of the current restriction
func (fn *TruncateFn) RestrictionSize(_ []byte, rest offsetrange.Restriction) float64 {
	return rest.Size()
}

// SplitRestriction is similar to the one used in checkpointing.go test.
func (fn *TruncateFn) SplitRestriction(_ []byte, rest offsetrange.Restriction) []offsetrange.Restriction {
	return rest.EvenSplits(2)
}

func (fn *TruncateFn) TruncateRestriction(rt *sdf.LockRTracker, _ []byte) offsetrange.Restriction {
	log.Debug(context.Background(), "triggering the truncate restriction")
	start := rt.GetRestriction().(offsetrange.Restriction).Start
	prevEnd := rt.GetRestriction().(offsetrange.Restriction).End
	newEnd := start + 20
	log.Infof(context.Background(), "TRUNCATING: truncating {Start:%v, End:%v} to {Start:%v, End:%v}", start, prevEnd, start, newEnd)
	return offsetrange.Restriction{
		Start: start,
		End:   newEnd,
	}
}

// ProcessElement continually gets the start position of the restriction and emits the element as it is.
func (fn *TruncateFn) ProcessElement(rt *sdf.LockRTracker, p []byte, emit func(int64)) sdf.ProcessContinuation {
	position := rt.GetRestriction().(offsetrange.Restriction).Start

	counter := 0
	for {
		if rt.TryClaim(position) {
			// Successful claim, emit the value and move on.
			log.Infof(context.Background(), "claimed pos: %v", position)
			emit(position)
			log.Infof(context.Background(), "claimed pos: %v, Rest: {Start: %v, End: %v}", position, rt.GetRestriction().(offsetrange.Restriction).Start, rt.GetRestriction().(offsetrange.Restriction).End)
			position++
			counter++
		} else if rt.GetError() != nil || rt.IsDone() {
			log.Info(context.Background(), "done processing, calling sdf.StopProcessing()")
			// Stop processing on error or completion
			if err := rt.GetError(); err != nil {
				log.Errorf(context.Background(), "error in restriction tracker, got %v", err)
			}
			return sdf.StopProcessing()
		} else {
			// Resume later.
			log.Info(context.Background(), "calling sdf.ResumeProcessing(5*time.Second)")
			return sdf.ResumeProcessingIn(5 * time.Second)
		}

		if counter >= 10 {
			log.Info(context.Background(), "10 counter reached, calling sdf.ResumeProcessing(1*time.Second)")
			return sdf.ResumeProcessingIn(1 * time.Second)
		}
		time.Sleep(1 * time.Second)
	}
}

func Drain(s beam.Scope) {
	beam.Init()

	s.Scope("truncate")
	beam.ParDo(s, &TruncateFn{}, beam.Impulse(s))
}
