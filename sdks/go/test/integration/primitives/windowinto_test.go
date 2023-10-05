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

func TestWindowSums_Lifted(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, WindowSums_Lifted)
}

func TestWindowSums_GBK(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, WindowSums_GBK)
}

func TestValidateWindowedSideInputs(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, ValidateWindowedSideInputs)
}

func TestTriggerDefault(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TriggerDefault)
}

func TestTriggerAlways(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TriggerAlways)
}

func TestTriggerElementCount(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TriggerElementCount)
}

func TestTriggerRepeat(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TriggerRepeat)
}

func TestTriggerAfterEndOfWindow(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TriggerAfterEndOfWindow)
}

func TestTriggerAfterAll(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TriggerAfterAll)
}

func TestTriggerAfterEach(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TriggerAfterEach)
}

func TestTriggerAfterAny(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TriggerAfterAny)
}

func TestTriggerAfterSynchronizedProcessingTime(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TriggerAfterSynchronizedProcessingTime)
}

func TestTriggerNever(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TriggerNever)
}

func TestTriggerOrFinally(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TriggerOrFinally)
}
