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
	store *Store // used to store states into state registry
}

// NewSampler creates a new state sampler.
func NewSampler(ctx context.Context, store *Store) StateSampler {
	return StateSampler{store: store}
}

func initialize() [4]*ExecutionState {
	var v [4]*ExecutionState
	for i := 0; i < 4; i++ {
		v[i] = &ExecutionState{}
	}
	return v
}

func (s *StateSampler) Sample(ctx context.Context, t time.Duration) {
	ps := loadPTransformState(ctx)
	pid := PTransformLabels(ps.pid)

	s.store.mu.Lock()
	defer s.store.mu.Unlock()

	if _, ok := s.store.stateRegistry[pid]; !ok {
		s.store.stateRegistry[pid] = initialize()
	}
	if v, ok := s.store.stateRegistry[pid]; ok {
		v[ps.state].TotalTime += t
		v[TotalBundle].TotalTime += t
	}

	if _, ok := s.store.executionStore[pid]; !ok {
		s.store.executionStore[pid] = &executionTracker{}
	}
	if v, ok := s.store.executionStore[pid]; ok {

		if v.transitionsAtLastSample != ps.transitions {
			// state change
			v.millisSinceLastTransition = 0
			v.numberOfTransitions = ps.transitions
			v.transitionsAtLastSample = ps.transitions
		} else {
			v.millisSinceLastTransition += t
		}
	}
}

// Start is called from the harness package repeatedly whenever required
func (s *StateSampler) Start(ctx context.Context, t time.Duration) {
	s.Sample(ctx, t)
}

func (s *StateSampler) Stop(ctx context.Context) {
	ps := loadPTransformState(ctx)
	pid := PTransformLabels(ps.pid)

	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if t := s.store.executionStore[pid].millisSinceLastTransition; t != 0 {
		if v, ok := s.store.stateRegistry[pid]; ok {
			v[ps.state].TotalTime += t
			v[TotalBundle].TotalTime += t
		}
	}
}
