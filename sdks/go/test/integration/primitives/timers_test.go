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

package primitives

import (
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/periodic"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

func TestTimers_EventTime_Bounded(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TimersEventTime(beam.Impulse))
}

func TestTimers_EventTime_Unbounded(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TimersEventTime(func(s beam.Scope) beam.PCollection {
		now := time.Now()
		return periodic.Impulse(s, now, now.Add(10*time.Second), 0, false)
	}))
}

// TODO(https://github.com/apache/beam/issues/29772): Add ProcessingTime Timer tests.
