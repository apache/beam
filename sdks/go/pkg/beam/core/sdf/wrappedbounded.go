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

package sdf

// WrappedTracker wraps an implementation of an RTracker and adds an IsBounded() function
// that returns true in order to allow RTrackers to be handled as bounded BoundableRTrackers
// if necessary (like in self-checkpointing evaluation.)
type WrappedTracker struct {
	RTracker
}

// IsBounded returns true, indicating that the underlying RTracker represents a bounded
// amount of work.
func (t *WrappedTracker) IsBounded() bool {
	return true
}

// NewWrappedTracker is a constructor for an RTracker that wraps another RTracker into a BoundedRTracker.
func NewWrappedTracker(underlying RTracker) *WrappedTracker {
	return &WrappedTracker{RTracker: underlying}
}
