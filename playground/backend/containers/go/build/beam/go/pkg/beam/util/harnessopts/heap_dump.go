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
	"strconv"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/hooks"
)

const (
	diagnosticsHook = "beam:go:hook:diagnostics:heapDump"
)

// HeapDumpFrequency sets the sampling frequency for how often the diagnostics service checks if it
// should take a heap dump and the maximum allowable time between heap dumps.
// Setting the sampling frequency to <=0 disables the heap dump checks.
// The default value for the sampling frequency is 1 second, the default max time is 60 seconds.
func HeapDumpFrequency(samplingFrequencySeconds, maxTimeBetweenDumpsSeconds int) error {
	if maxTimeBetweenDumpsSeconds < samplingFrequencySeconds {
		return fmt.Errorf("max time between dumps %v should be greater than or equal to sampling frequence %v", maxTimeBetweenDumpsSeconds, samplingFrequencySeconds)
	}
	// The hook itself is defined in beam/core/runtime/harness/init/diagnostics_hook.go
	return hooks.EnableHook(diagnosticsHook, strconv.Itoa(samplingFrequencySeconds), strconv.Itoa(maxTimeBetweenDumpsSeconds))
}
