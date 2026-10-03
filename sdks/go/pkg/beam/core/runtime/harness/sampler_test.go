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

package harness

import (
	"context"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
)

func TestNewSampler(t *testing.T) {
	ctx := metrics.SetBundleID(context.Background(), "test-bundle")
	store := metrics.GetStore(ctx)
	if store == nil {
		t.Fatal("GetStore returned nil")
	}

	s := newSampler(store, 0)
	if s == nil {
		t.Fatal("newSampler returned nil")
	}
	if s.done == nil {
		t.Error("sampler done channel is nil")
	}
}

func TestStateSampler_Stop(t *testing.T) {
	ctx := metrics.SetBundleID(context.Background(), "test-bundle")
	store := metrics.GetStore(ctx)
	s := newSampler(store, 0)
	// stop should not panic when called on a properly initialized sampler.
	s.stop()
}

func TestStateSampler_Start_ContextCancel(t *testing.T) {
	ctx := metrics.SetBundleID(context.Background(), "test-bundle")
	store := metrics.GetStore(ctx)
	s := newSampler(store, 0)

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel() // Cancel immediately

	// start should return nil immediately when context is already canceled.
	err := s.start(cancelCtx, samplePeriod)
	if err != nil {
		t.Errorf("start returned error on canceled context: %v", err)
	}
}

func TestStateSampler_Start_DoneChannel(t *testing.T) {
	ctx := metrics.SetBundleID(context.Background(), "test-bundle")
	store := metrics.GetStore(ctx)
	s := newSampler(store, 0)

	// Signal the done channel after a short delay.
	go func() {
		time.Sleep(50 * time.Millisecond)
		s.stop()
	}()

	err := s.start(context.Background(), 100*time.Millisecond)
	if err != nil {
		t.Errorf("start returned error on done signal: %v", err)
	}
}
