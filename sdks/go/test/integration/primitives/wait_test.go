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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

func TestWaitStreamSameFixedWindows(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, WaitStreamSameFixedWindows)
}

func TestWaitStreamDifferentFixedWindows(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, WaitStreamDifferentFixedWindows)
}

func TestWaitStreamSlidingSignal(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, WaitStreamSlidingSignal)
}

func TestWaitStreamSomeSignalWindowsEmpty(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, WaitStreamSomeSignalWindowsEmpty)
}

func TestWaitStreamSequencing(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, WaitStreamSequencing)
}

func TestWaitStreamSequencingAllowedLateness(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, WaitStreamSequencingAllowedLateness)
}

func TestWaitStreamSequencingMultipleSignals(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, WaitStreamSequencingMultipleSignals)
}

func TestWaitStreamSequencingMultipleSignalsLenientFirst(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, WaitStreamSequencingMultipleSignalsLenientFirst)
}

func TestWaitBoundedGlobalWindow(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, WaitBoundedGlobalWindow)
}

func TestWaitBoundedFixedWindows(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, WaitBoundedFixedWindows)
}
