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
	"math"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn3x1[*sdf.LockRTracker, []byte, func(int64), sdf.ProcessContinuation](&TruncateFn{})

	register.Emitter1[int64]()
}

// RangeEstimator implements the offsetrange.RangeEndEstimator interface.
// It provides the estimated end for a restriction.
type RangeEstimator struct {
	end int64
}

// Estimate returns the estimated end.
func (r *RangeEstimator) Estimate() int64 {
	return r.end
}

// SetEstimate sets the estimated end.
func (r *RangeEstimator) SetEstimate(estimate int64) {
	r.end = estimate
}

// TruncateFn is an SDF.
type TruncateFn struct {
	Estimator RangeEstimator
}

// CreateInitialRestriction creates an initial restriction
func (fn *TruncateFn) CreateInitialRestriction(_ []byte) offsetrange.Restriction {
	return offsetrange.Restriction{
		Start: int64(1),
		End:   int64(math.MaxInt64),
	}
}

// CreateTracker wraps the given restriction into a LockRTracker type.
func (fn *TruncateFn) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
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

// TruncateRestriction truncates the restriction during drain.
func (fn *TruncateFn) TruncateRestriction(rt *sdf.LockRTracker, _ []byte) offsetrange.Restriction {
	start := rt.GetRestriction().(offsetrange.Restriction).Start
	newEnd := start + 20
	return offsetrange.Restriction{
		Start: start,
		End:   newEnd,
	}
}

// ProcessElement continually gets the start position of the restriction and emits the element as it is.
func (fn *TruncateFn) ProcessElement(rt *sdf.LockRTracker, _ []byte, emit func(int64)) sdf.ProcessContinuation {
	position := rt.GetRestriction().(offsetrange.Restriction).Start
	counter := 0
	for {
		if rt.TryClaim(position) {
			// Successful claim, emit the value and move on.
			emit(position)
			position++
			counter++
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

		if counter >= 10 {
			return sdf.ResumeProcessingIn(1 * time.Second)
		}
		time.Sleep(1 * time.Second)
	}
}

// Drain tests the SDF truncation during drain.
func Drain(s beam.Scope) {
	beam.Init()
	s.Scope("truncate")
	beam.ParDo(s, &TruncateFn{}, beam.Impulse(s))
}
