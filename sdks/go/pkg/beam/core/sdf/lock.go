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

import "sync"

// NewLockRTracker creates a LockRTracker initialized with the specified
// restriction tracker as its underlying restriction tracker.
func NewLockRTracker(rt RTracker) *LockRTracker {
	return &LockRTracker{Rt: rt}
}

// LockRTracker is a restriction tracker that wraps another restriction
// tracker and adds thread safety to it by locking a mutex in each method,
// before delegating to the underlying tracker.
type LockRTracker struct {
	Mu sync.Mutex // Lock on accessing underlying tracker.
	// The underlying tracker. If accessing directly, consider thread safety.
	// Lock the mutex if thread safety is needed.
	Rt RTracker
}

// TryClaim locks a mutex for thread safety, and then delegates to the
// underlying tracker's TryClaim.
func (rt *LockRTracker) TryClaim(pos interface{}) (ok bool) {
	rt.Mu.Lock()
	defer rt.Mu.Unlock()
	return rt.Rt.TryClaim(pos)
}

// GetError locks a mutex for thread safety, and then delegates to the
// underlying tracker's GetError.
func (rt *LockRTracker) GetError() error {
	rt.Mu.Lock()
	defer rt.Mu.Unlock()
	return rt.Rt.GetError()
}

// TrySplit locks a mutex for thread safety, and then delegates to the
// underlying tracker's TrySplit.
func (rt *LockRTracker) TrySplit(fraction float64) (interface{}, interface{}, error) {
	rt.Mu.Lock()
	defer rt.Mu.Unlock()
	return rt.Rt.TrySplit(fraction)
}

// GetProgress locks a mutex for thread safety, and then delegates to the
// underlying tracker's GetProgress.
func (rt *LockRTracker) GetProgress() (float64, float64) {
	rt.Mu.Lock()
	defer rt.Mu.Unlock()
	return rt.Rt.GetProgress()
}

// IsDone locks a mutex for thread safety, and then delegates to the
// underlying tracker's IsDone.
func (rt *LockRTracker) IsDone() bool {
	rt.Mu.Lock()
	defer rt.Mu.Unlock()
	return rt.Rt.IsDone()
}

// GetRestriction locks a mutex for thread safety, and then delegates to the
// underlying tracker's GetRestriction.
func (rt *LockRTracker) GetRestriction() interface{} {
	rt.Mu.Lock()
	defer rt.Mu.Unlock()
	return rt.Rt.GetRestriction()
}
