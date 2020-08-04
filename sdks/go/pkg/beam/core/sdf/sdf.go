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

// Package contains interfaces used specifically for splittable DoFns.
//
// Warning: Splittable DoFns are still experimental, largely untested, and
// likely to have bugs.
package sdf

// RTracker is an interface used to interact with restrictions while processing elements in
// splittable DoFns (specifically, in the ProcessElement method). Each RTracker tracks the progress
// of a single restriction.
//
// All RTracker methods should be thread-safe for dynamic splits to function correctly.
type RTracker interface {
	// TryClaim attempts to claim the block of work located in the given position of the
	// restriction. This method must be called in ProcessElement to claim work before it can be
	// processed. Processing work without claiming it first can lead to incorrect output.
	//
	// The position type is up to individual implementations, and will usually be related to the
	// kind of restriction used. For example, a simple restriction representing a numeric range
	// might use an int64. A more complex restriction, such as one representing a multidimensional
	// space, might use a more complex type.
	//
	// If the claim is successful, the DoFn must process the entire block. If the claim is
	// unsuccessful ProcessElement method of the DoFn must return without performing
	// any additional work or emitting any outputs.
	//
	// If the claim fails due to an error, that error is stored and will be automatically emitted
	// when the RTracker is validated, or can be manually retrieved with GetError.
	//
	// This pseudocode example illustrates the typical usage of TryClaim:
	//
	// 	pos = position of first block within the restriction
	// 	for TryClaim(pos) == true {
	// 		// Do all work in the claimed block and emit outputs.
	// 		pos = position of next block within the restriction
	// 	}
	// 	return
	TryClaim(pos interface{}) (ok bool)

	// GetError returns the error that made this RTracker stop executing, and returns nil if no
	// error occurred. This is the error that is emitted if automated validation fails.
	GetError() error

	// TrySplit splits the current restriction into a primary (currently executing work) and
	// residual (work to be split off) based on a fraction of work remaining. The split is performed
	// at the first valid split point located after the given fraction of remaining work.
	//
	// For example, a fraction of 0.5 means to split at the halfway point of remaining work only. If
	// 50% of work is done and 50% remaining, then a fraction of 0.5 would split after 75% of work.
	//
	// This method modifies the underlying restriction in the RTracker to reflect the primary. It
	// then returns a copy of the newly modified restriction as a primary, and returns a new
	// restriction for the residual. If the split would produce an empty residual (either because
	// the only split point is the end of the restriction, or the split failed for some recoverable
	// reason), then this function returns nil as the residual.
	//
	// If an error is returned, some catastrophic failure occurred and the entire bundle will fail.
	TrySplit(fraction float64) (primary, residual interface{}, err error)

	// GetProgress returns two abstract scalars representing the amount of done and remaining work.
	// These values have no specific units, but are used to estimate work in relation to each other
	// and should be self-consistent.
	GetProgress() (done float64, remaining float64)

	// IsDone returns a boolean indicating whether all blocks inside the restriction have been
	// claimed. This method is called by the SDK Harness to validate that a splittable DoFn has
	// correctly processed all work in a restriction before finishing. If this method still returns
	// false after processing, then GetError is expected to return a non-nil error.
	IsDone() bool

	// GetRestriction returns the restriction this tracker is tracking, or nil if the restriction
	// is unavailable for some reason.
	GetRestriction() interface{}
}
