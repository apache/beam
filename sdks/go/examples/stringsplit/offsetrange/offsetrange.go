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
package offsetrange

import (
	"errors"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*Tracker)(nil)))
	beam.RegisterType(reflect.TypeOf((*Restriction)(nil)))
}

type Restriction struct {
	Start, End int64 // Half-closed interval with boundaries [start, end).
}

// Tracker tracks a restriction  that can be represented as a range of integer values,
// for example for byte offsets in a file, or indices in an array. Note that this tracker makes
// no assumptions about the positions of blocks within the range, so users must handle validation
// of block positions if needed.
type Tracker struct {
	Rest    Restriction
	Claimed int64 // Tracks the last claimed position.
	Stopped bool  // Tracks whether TryClaim has already indicated to stop processing elements for
	// any reason.
	Err error
}

// NewTracker is a constructor for an Tracker given a start and end range.
func NewTracker(rest Restriction) *Tracker {
	return &Tracker{
		Rest:    rest,
		Claimed: rest.Start - 1,
		Stopped: false,
		Err:     nil,
	}
}

// TryClaim accepts an int64 position and successfully claims it if that position is greater than
// the previously claimed position and less than the end of the restriction. Note that the
// Tracker is not considered done until a position >= tracker.end tries to be claimed,
// at which point this method signals to end processing.
func (tracker *Tracker) TryClaim(rawPos interface{}) bool {
	if tracker.Stopped == true {
		tracker.Err = errors.New("cannot claim work after restriction tracker returns false")
		return false
	}

	pos := rawPos.(int64)

	if pos < tracker.Rest.Start {
		tracker.Stopped = true
		tracker.Err = errors.New("position claimed is out of bounds of the restriction")
		return false
	}
	if pos <= tracker.Claimed {
		tracker.Stopped = true
		tracker.Err = errors.New("cannot claim a position lower than the previously claimed position")
		return false
	}

	tracker.Claimed = pos
	if pos >= tracker.Rest.End {
		tracker.Stopped = true
		return false
	}
	return true
}

// IsDone returns true if the most recent claimed element is past the end of the restriction.
func (tracker *Tracker) GetError() error {
	return tracker.Err
}

// TrySplit splits at the nearest integer greater than the given fraction of the remainder. If the
// fraction given is outside of the [0, 1] range, it is clamped to 0 or 1.
func (tracker *Tracker) TrySplit(fraction float64) (primary, residual interface{}, err error) {
	if tracker.Stopped || tracker.IsDone() {
		return tracker.Rest, nil, nil
	}
	if fraction < 0 {
		fraction = 0
	} else if fraction > 1 {
		fraction = 1
	}

	splitPt := tracker.Claimed + int64(fraction*float64(tracker.Rest.End-tracker.Claimed))
	if splitPt >= tracker.Rest.End {
		return tracker.Rest, nil, nil
	}
	residual = Restriction{splitPt, tracker.Rest.End}
	tracker.Rest.End = splitPt
	return tracker.Rest, residual, nil
}

// GetProgress reports progress based on the claimed size and unclaimed sizes of the restriction.
func (tracker *Tracker) GetProgress() (done, remaining float64) {
	done = float64(tracker.Claimed - tracker.Rest.Start)
	remaining = float64(tracker.Rest.End - tracker.Claimed)
	return
}

// IsDone returns true if the most recent claimed element is past the end of the restriction.
func (tracker *Tracker) IsDone() bool {
	return tracker.Claimed >= tracker.Rest.End
}
