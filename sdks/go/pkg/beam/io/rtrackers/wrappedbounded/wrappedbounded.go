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

package wrappedbounded

import "github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"

// Tracker wraps an implementation of an RTracker and adds an IsBounded() function
// that returns true in order to allow RTrackers to be handled as bounded BoundableRTrackers
// if necessary (like in self-checkpointing evaluation.)
type Tracker struct {
	baseTracker sdf.RTracker
}

// TryClaim attempts to claim a block of work from the underlying RTracker's restriction.
func (t *Tracker) TryClaim(pos interface{}) (ok bool) {
	return t.baseTracker.TryClaim(pos)
}

// GetError returns an error from the underlying RTracker if it has stopped executing. Returns nil
// if none has occurred.
func (t *Tracker) GetError() error {
	return t.baseTracker.GetError()
}

// TrySplit splits the underlying RTracker's restriction into a primary (work that is currently executing)
// and a residual (work that will be split off and resumed later.)
func (t *Tracker) TrySplit(fraction float64) (primary, residual interface{}, err error) {
	return t.baseTracker.TrySplit(fraction)
}

// GetProgress returns two abstract scalars representing the amount of work done and the remaining work
// left in the underlying RTracker. These are unitless values, only used to estimate work in relation to
// each other.
func (t *Tracker) GetProgress() (done float64, remaining float64) {
	return t.baseTracker.GetProgress()
}

// IsDone() returns a boolean indicating if the work represented by the underlying RTracker has
// been completed.
func (t *Tracker) IsDone() bool {
	return t.baseTracker.IsDone()
}

// GetRestriction returns the restriction maintained by the underlying RTracker.
func (t *Tracker) GetRestriction() interface{} {
	return t.baseTracker.GetRestriction()
}

// IsBounded returns true, indicating that the underlying RTracker represents a bounded
// amount of work.
func (t *Tracker) IsBounded() bool {
	return true
}

// NewTracker is a constructor for an RTracker that wraps another RTracker into a BoundedRTracker.
func NewTracker(underlying sdf.RTracker) *Tracker {
	return &Tracker{baseTracker: underlying}
}
