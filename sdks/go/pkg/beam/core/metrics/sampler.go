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
	"unsafe"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

// StateSampler tracks the state of a bundle.
type StateSampler struct {
	store                     *Store // used to store states into state registry
	millisSinceLastTransition time.Duration
	transitionsAtLastSample   int64
	nextLogTime               time.Duration
	logInterval               time.Duration
}

// NewSampler creates a new state sampler.
func NewSampler(store *Store) StateSampler {
	return StateSampler{store: store, nextLogTime: 5 * time.Minute, logInterval: 5 * time.Minute}
}

// Sample checks for state transition in processing a DoFn
func (s *StateSampler) Sample(ctx context.Context, t time.Duration) {
	ps := loadCurrentState(s)
	if ps.pid == "" {
		return
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()

	if v, ok := s.store.stateRegistry[ps.pid]; ok {
		v[ps.state].TotalTime += t
		v[TotalBundle].TotalTime += t

		if s.transitionsAtLastSample != ps.transitions {
			// state change detected
			s.millisSinceLastTransition = 0
			s.transitionsAtLastSample = ps.transitions
		} else {
			s.millisSinceLastTransition += t
		}

		if s.millisSinceLastTransition > s.nextLogTime {
			log.Info(ctx, "...standard long running operation log ...", ps.pid, ps.state, s.millisSinceLastTransition)
			s.nextLogTime += s.logInterval
		}
	}
}

func loadCurrentState(s *StateSampler) currentStateVal {
	if ts := (atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&s.store.bundleState)))); ts == nil {
		return currentStateVal{}
	} else {
		bs := *(*BundleState)(ts)
		return currentStateVal{pid: bs.pid, state: bs.currentState, transitions: atomic.LoadInt64(s.store.transitions)}
	}
}

type PTransformState struct {
	states [3]BundleState
}

func NewPTransformState(pid string) *PTransformState {
	return &PTransformState{
		states: [3]BundleState{
			{pid, StartBundle},
			{pid, ProcessBundle},
			{pid, FinishBundle},
		},
	}
}

func SetPTransform(ctx context.Context) {

}
