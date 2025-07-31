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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"time"
)

type stateSampler struct {
	done    chan (int)
	sampler metrics.StateSampler
}

func newSampler(store *metrics.Store, elementProcessingTimeout time.Duration) *stateSampler {
	return &stateSampler{sampler: metrics.NewSampler(store, elementProcessingTimeout), done: make(chan int)}
}

func (s *stateSampler) start(ctx context.Context, t time.Duration) error {
	ticker := time.NewTicker(t)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return nil
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := s.sampler.Sample(ctx, t)
			if err != nil {
				return errors.Errorf("Failed to sample: %v", err)
			}
		}
	}
}

func (s *stateSampler) stop() {
	close(s.done)
}
