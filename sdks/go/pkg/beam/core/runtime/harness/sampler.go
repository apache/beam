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
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
)

type stateSampler struct {
	done    chan (int)
	sampler metrics.StateSampler
}

func newSampler(store *metrics.Store) *stateSampler {
	return &stateSampler{sampler: metrics.NewSampler(store), done: make(chan int)}
}

func (s *stateSampler) start(ctx context.Context, t time.Duration) {
	ticker := time.NewTicker(t)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sampler.Sample(ctx, t)
		}
	}
}

func (s *stateSampler) stop() {
	close(s.done)
}
