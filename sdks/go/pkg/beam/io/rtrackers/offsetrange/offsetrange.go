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
package offsetrange

import (
	"errors"
	"math"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*Tracker)(nil)))
	beam.RegisterType(reflect.TypeOf((*Restriction)(nil)))
}

// Restriction is an offset range restriction, which represents a range of
// integers as a half-closed interval with boundaries [start, end).
type Restriction struct {
	Start, End int64
}

// EvenSplits splits a restriction into a number of evenly sized restrictions.
// Each split restriction is guaranteed to not be empty, and each unit from the
// original restriction is guaranteed to be contained in one split restriction.
//
// Num should be greater than 0. Otherwise there is no way to split the
// restriction and this function will return the original restriction.
func (r *Restriction) EvenSplits(num int64) (splits []Restriction) {
	if num <= 1 {
		// Don't split, just return original restriction.
		return append(splits, *r)
	}

	offset := r.Start
	size := r.End - r.Start
	for i := int64(0); i < num; i++ {
		split := Restriction{
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

// Size returns the restriction's size as the difference between Start and End.
func (r *Restriction) Size() float64 {
	return float64(r.End - r.Start)
}

// Tracker tracks a restriction  that can be represented as a range of integer values,
// for example for byte offsets in a file, or indices in an array. Note that this tracker makes
// no assumptions about the positions of blocks within the range, so users must handle validation
// of block positions if needed.
type Tracker struct {
	Rest    Restriction
	Claimed int64 // Tracks the last claimed position.
	Stopped bool  // Tracks whether TryClaim has indicated to stop processing elements.
	Err     error
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

// TryClaim accepts an int64 position representing the starting position of a block of work. It
// successfully claims it if the position is greater than the previously claimed position and within
// the restriction. Claiming a position at or beyond the end of the restriction signals that the
// entire restriction has been processed and is now done, at which point this method signals to end
// processing.
//
// The tracker stops with an error if a claim is attempted after the tracker has signalled to stop,
// if a position is claimed before the start of the restriction, or if a position is claimed before
// the latest successfully claimed.
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

// GetError returns the error that caused the tracker to stop, if there is one.
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

	// Use Ceil to always round up from float split point.
	splitPt := tracker.Claimed + int64(math.Ceil(fraction*float64(tracker.Rest.End-tracker.Claimed)))
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
	return tracker.Err == nil && tracker.Claimed >= tracker.Rest.End
}
