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
	"sync/atomic"
	"time"
)

// StateSampler tracks the state of a bundle.
type StateSampler struct {
	done  chan (int) // signal to stop sampling
	e     *ExecutionTracker
	store *Store
	total int64
}

// NewSampler creates a new state sampler.
func NewSampler(ctx context.Context, store *Store) StateSampler {
	return StateSampler{done: make(chan int), e: getExecutionStore(ctx), store: store}
}

// Start is called from the harness package repeatedly whenever required
func (s *StateSampler) Start(ctx context.Context, t time.Duration) {
	s.startSampler(ctx, t)
}

func (s *StateSampler) startSampler(ctx context.Context, t time.Duration) {
	if loadTransitions(ctx) != atomic.LoadInt64(&(s.e.TransitionsAtLastSample)) {
		s.sample(ctx)
	}

	atomic.AddInt64(&s.e.MillisSinceLastTransition, int64(t))
	atomic.AddInt64(&s.e.State.TotalTimeMillis, int64(t))
}

func (s *StateSampler) Stop(ctx context.Context, t time.Duration) {
	s.stopSampler(ctx, t)
}

func (s *StateSampler) stopSampler(ctx context.Context, t time.Duration) {
	// collect the remaining metrics (finish bundle metrics)
	s.sample(ctx)
	// add final state

	ex := ExecutionState{State: TotalBundle, TotalTimeMillis: s.total}
	s.store.AddState(ex, loadPTransformState(ctx).pid)
}

func (s *StateSampler) sample(ctx context.Context) {
	ps := loadPTransformState(ctx)
	t := loadTransitions(ctx)
	s.store.AddState(s.e.State, ps.pid)
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	s.total += s.e.State.TotalTimeMillis
	s.e.TransitionsAtLastSample = t
	s.e.State.TotalTimeMillis = s.e.MillisSinceLastTransition - s.e.State.TotalTimeMillis
	s.e.State.State = ps.state
}
