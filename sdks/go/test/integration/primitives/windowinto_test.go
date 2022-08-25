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

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

func TestWindowSums_Lifted(t *testing.T) {
	integration.CheckFilters(t)
	p, s := beam.NewPipelineWithRoot()
	WindowSums_Lifted(s)
	ptest.RunAndValidate(t, p)
}

func TestWindowSums_GBK(t *testing.T) {
	integration.CheckFilters(t)
	p, s := beam.NewPipelineWithRoot()
	WindowSums_GBK(s)
	ptest.RunAndValidate(t, p)
}

func TestValidateWindowedSideInputs(t *testing.T) {
	integration.CheckFilters(t)
	p, s := beam.NewPipelineWithRoot()
	ValidateWindowedSideInputs(s)
	ptest.RunAndValidate(t, p)
}

func TestTriggerDefault(t *testing.T) {
	integration.CheckFilters(t)
	p, s := beam.NewPipelineWithRoot()
	TriggerDefault(s)
	ptest.RunAndValidate(t, p)
}

func TestTriggerAlways(t *testing.T) {
	integration.CheckFilters(t)
	p, s := beam.NewPipelineWithRoot()
	TriggerAlways(s)
	ptest.RunAndValidate(t, p)
}

func TestTriggerElementCount(t *testing.T) {
	integration.CheckFilters(t)
	p, s := beam.NewPipelineWithRoot()
	TriggerElementCount(s)
	ptest.RunAndValidate(t, p)
}

func TestTriggerRepeat(t *testing.T) {
	integration.CheckFilters(t)
	p, s := beam.NewPipelineWithRoot()
	TriggerRepeat(s)
	ptest.RunAndValidate(t, p)
}

func TestTriggerAfterEndOfWindow(t *testing.T) {
	integration.CheckFilters(t)
	p, s := beam.NewPipelineWithRoot()
	TriggerAfterEndOfWindow(s)
	ptest.RunAndValidate(t, p)
}

func TestTriggerAfterAll(t *testing.T) {
	integration.CheckFilters(t)
	p, s := beam.NewPipelineWithRoot()
	TriggerAfterAll(s)
	ptest.RunAndValidate(t, p)
}

func TestTriggerAfterEach(t *testing.T) {
	integration.CheckFilters(t)
	p, s := beam.NewPipelineWithRoot()
	TriggerAfterEach(s)
	ptest.RunAndValidate(t, p)
}

func TestTriggerAfterAny(t *testing.T) {
	integration.CheckFilters(t)
	p, s := beam.NewPipelineWithRoot()
	TriggerAfterAny(s)
	ptest.RunAndValidate(t, p)
}

func TestTriggerAfterSynchronizedProcessingTime(t *testing.T) {
	integration.CheckFilters(t)
	p, s := beam.NewPipelineWithRoot()
	TriggerAfterSynchronizedProcessingTime(s)
	ptest.RunAndValidate(t, p)
}

func TestTriggerNever(t *testing.T) {
	integration.CheckFilters(t)
	p, s := beam.NewPipelineWithRoot()
	TriggerNever(s)
	ptest.RunAndValidate(t, p)
}

func TestTriggerOrFinally(t *testing.T) {
	integration.CheckFilters(t)
	p, s := beam.NewPipelineWithRoot()
	TriggerOrFinally(s)
	ptest.RunAndValidate(t, p)
}
