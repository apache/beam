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

// Package offsetrange defines a restriction and restriction tracker for offset
// ranges. An offset range is just a range, with a start and end, that can
// begin at an offset, and is commonly used to represent byte ranges for files
// or indices for iterable containers.
package growableOffsetrange

import (
	"math"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
)

func init() {
	runtime.RegisterType(reflect.TypeOf((*Tracker)(nil)))
}

type RangeEndEstimator interface {
	Estimate() int64
}

// Tracker tracks a restriction  that can be represented as a range of integer values,
// for example for byte offsets in a file, or indices in an array. Note that this tracker makes
// no assumptions about the positions of blocks within the range, so users must handle validation
// of block positions if needed.
type Tracker struct {
	offsetrange.Tracker
	rangeEndEstimator RangeEndEstimator
}

// NewTracker is a constructor for an Tracker given a start and end range.
func NewTracker(start int64, rangeEndEstimator RangeEndEstimator) *Tracker {
	return &Tracker{*offsetrange.NewTracker(offsetrange.Restriction{Start: start, End: math.MaxInt64}), rangeEndEstimator}
}

func (tracker *Tracker) Start() int64 {
	return tracker.GetRestriction().(offsetrange.Restriction).Start
}

func (tracker *Tracker) End() int64 {
	return tracker.GetRestriction().(offsetrange.Restriction).End
}

func max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

// TrySplit splits at the nearest integer greater than the given fraction of the remainder. If the
// fraction given is outside of the [0, 1] range, it is clamped to 0 or 1.
func (tracker *Tracker) TrySplit(fraction float64) (primary, residual interface{}, err error) {
	if tracker.End() != math.MaxInt64 || tracker.Start() == tracker.End() {
		return tracker.Tracker.TrySplit(fraction)
	}

	if tracker.GetAttempted() != -1 && tracker.GetAttempted() == math.MaxInt64 {
		return nil, nil, nil
	}

	var cur int64
	if tracker.GetAttempted() != -1 {
		cur = tracker.GetAttempted()
	} else {
		cur = tracker.Start() - 1
	}

	estimatedEnd := max(tracker.rangeEndEstimator.Estimate(), 1)

	splitPt := cur + int64(math.Ceil(math.Max(1, float64(estimatedEnd-cur)*(fraction))))
	if splitPt >= estimatedEnd {
		return nil, nil, nil
	}
	residual = offsetrange.Restriction{Start: splitPt, End: tracker.End()}
	rest := tracker.GetRestriction().(offsetrange.Restriction)
	rest.End = splitPt
	return tracker.GetRestriction(), residual, nil
}

// GetProgress reports progress based on the claimed size and unclaimed sizes of the restriction.
func (tracker *Tracker) GetProgress() (done, remaining float64) {
	if tracker.End() != math.MaxInt64 || tracker.End() == tracker.Start() {
		return tracker.Tracker.GetProgress()
	}

	estimatedEnd := tracker.rangeEndEstimator.Estimate()

	if tracker.GetAttempted() == -1 {
		done = 0
		remaining = math.Max(float64(estimatedEnd)-float64(tracker.Start()), 0)
		return
	}

	done = float64((tracker.GetClaimed() + 1) - tracker.Start())
	remaining = math.Max(float64(estimatedEnd-(tracker.GetAttempted())), 0)
	return
}

func (tracker *Tracker) IsBounded() bool {
	if tracker.GetAttempted() != -1 && tracker.GetAttempted() == math.MaxInt64 {
		return true
	}
	if tracker.End() == math.MaxInt64 {
		return false
	}
	return true
}
