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
	done  chan (int) // signal to stop sampling
	e     *ExecutionTracker
	store *Store
	pid   string // PTransform ID
}

// NewSampler creates a new state sampler.
func NewSampler(ctx context.Context, store *Store, pid string) StateSampler {
	return StateSampler{done: make(chan int), e: GetExecutionStore(ctx), store: store, pid: pid}
}

func (s *StateSampler) Start() {
	s.startSampler()
}

func (s *StateSampler) startSampler() {
	for {
		select {
		case <-s.done:
			return
		default:
			if s.e.NumberOfTransitions != s.e.TransitionsAtLastSample {
				s.sample()
			}

			// TODO: constant for sampling period
			s.e.MillisSinceLastTransition += 200
			s.e.State.TotalTimeMillis += 200
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func (s *StateSampler) Stop() {
	s.stopSampler()
}

func (s *StateSampler) stopSampler() {
	s.done <- 1
	// collect the remaining metrics (finish bundle metrics)
	s.sample()
}

func (s *StateSampler) sample() {
	s.e.TransitionsAtLastSample = s.e.NumberOfTransitions
	s.e.MillisSinceLastTransition = 0
	s.store.AddState(s.e.State, s.pid)
	s.e.State.State = s.e.CurrentState
	s.e.State.TotalTimeMillis = 0
}
