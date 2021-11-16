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

package harnessopts

import (
	"fmt"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/hooks"
)

const (
	samplePeriodHook = "beam:go:hook:dofnmetrics:sampletime"
)

// SampleInterval sets the sampling time period (greater than 1ms) for DoFn metrics sampling.
// Default value is 200ms.
func SampleInterval(samplePeriod time.Duration) error {
	if samplePeriod < time.Millisecond {
		return fmt.Errorf("sample period should be greater than 1ms, got %v", samplePeriod)
	}
	sampleTime := samplePeriod.String()
	// The hook itself is defined in beam/core/runtime/harness/sampler_hook.go
	return hooks.EnableHook(samplePeriodHook, sampleTime)
}
