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

package metrics

import (
	"context"
	"time"
)

// StateSampler tracks the state of a bundle.
type StateSampler struct {
	store                     *Store // used to store states into state registry
	millisSinceLastTransition time.Duration
	transitionsAtLastSample   int64
}

// NewSampler creates a new state sampler.
func NewSampler(ctx context.Context, store *Store) StateSampler {
	return StateSampler{store: store}
}

func (s *StateSampler) Sample(ctx context.Context, t time.Duration) {
	ps := loadCurrentState(ctx)
	s.store.mu.Lock()
	defer s.store.mu.Unlock()

	if v, ok := s.store.stateRegistry[ps.pid]; ok {
		v[ps.state].TotalTime += t
		v[TotalBundle].TotalTime += t

		e := s.store

		if s.transitionsAtLastSample != ps.transitions {
			// state change detected
			s.millisSinceLastTransition = 0
			e.transitions = ps.transitions
			s.transitionsAtLastSample = ps.transitions
		} else {
			s.millisSinceLastTransition += t
		}
	}
}
