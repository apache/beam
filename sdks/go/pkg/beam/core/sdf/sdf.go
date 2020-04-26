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

// Package sdf is experimental, incomplete, and not yet meant for general usage.
package sdf

// RTracker is an interface used to interact with restrictions while processing elements in
// SplittableDoFns. Each implementation of RTracker is expected to be used for tracking a single
// restriction type, which is the type that should be used to create the RTracker, and output by
// TrySplit.
type RTracker interface {
	// TryClaim attempts to claim the block of work in the current restriction located at a given
	// position. This method must be used in the ProcessElement method of Splittable DoFns to claim
	// work before performing it. If no work is claimed, the ProcessElement is not allowed to perform
	// work or emit outputs. If the claim is successful, the DoFn must process the entire block. If
	// the claim is unsuccessful the ProcessElement method of the DoFn must return without performing
	// any additional work or emitting any outputs.
	//
	// TryClaim accepts an arbitrary value that can be interpreted as the position of a block, and
	// returns a boolean indicating whether the claim succeeded.
	//
	// If the claim fails due to an error, that error can be retrieved with GetError.
	//
	// For SDFs to work properly, claims must always be monotonically increasing in reference to the
	// restriction's start and end points, and every block of work in a restriction must be claimed.
	//
	// This pseudocode example illustrates the typical usage of TryClaim:
	//
	// 	pos = position of first block after restriction.start
	// 	for TryClaim(pos) == true {
	// 		// Do all work in the claimed block and emit outputs.
	// 		pos = position of next block
	// 	}
	// 	return
	TryClaim(pos interface{}) (ok bool)

	// GetError returns the error that made this RTracker stop executing, and it returns nil if no
	// error occurred. If IsDone fails while validating this RTracker, this method will be
	// called to log the error.
	GetError() error

	// TrySplit splits the current restriction into a primary and residual based on a fraction of the
	// work remaining. The split is performed along the first valid split point located after the
	// given fraction of the remainder. This method is called by the SDK harness when receiving a
	// split request by the runner.
	//
	// The current restriction is split into two by modifying the current restriction's endpoint to
	// turn it into the primary, and returning a new restriction tracker representing the residual.
	// If no valid split point exists, this method returns nil instead of a residual, but does not
	// return an error. If this method is unable to split due to some error then it returns nil and
	// an error.
	TrySplit(fraction float64) (residual interface{}, err error)

	// GetProgress returns two abstract scalars representing the amount of done and remaining work.
	// These values have no specific units, but are used to estimate work in relation to each other
	// and should be self-consistent.
	GetProgress() (done float64, remaining float64)

	// IsDone returns a boolean indicating whether all blocks inside the restriction have been
	// claimed. This method is called by the SDK Harness to validate that a Splittable DoFn has
	// correctly processed all work in a restriction before finishing.
	IsDone() bool
}
