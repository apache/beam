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

// Package errorx contains utilities for handling errors.
package errorx

import "sync"

// GuardedError is a concurrency-safe error wrapper. It is sticky
// in that the first error won't be overwritten.
type GuardedError struct {
	err error
	mu  sync.Mutex
}

// Error returns the guarded error.
func (g *GuardedError) Error() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.err
}

// TrySetError sets the error, if not already set. Returns true iff the
// error was set.
func (g *GuardedError) TrySetError(err error) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	upd := (g.err == nil)
	if upd {
		g.err = err
	}
	return upd
}
