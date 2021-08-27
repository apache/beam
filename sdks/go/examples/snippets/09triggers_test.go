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

// Package snippets contains code used in the Beam Programming Guide
// as examples for the Apache Beam Go SDK. These snippets are compiled
// and their tests run to ensure correctness. However, due to their
// piecemeal pedagogical use, they may not be the best example of
// production code.
//
// The Beam Programming Guide can be found at https://beam.apache.org/documentation/programming-guide/.
package snippets

import (
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

func TestTriggerElementCount(t *testing.T) {
	integration.CheckFilters(t)
	p, s := beam.NewPipelineWithRoot()
	sums := TriggerElementCount(s)
	sums = beam.WindowInto(s, window.NewGlobalWindows(), sums)
	passert.Count(s, sums, "total collections", 1)
	ptest.RunAndValidate(t, p)
}

func TestTriggerAfterProcessingTime(t *testing.T) {
	integration.CheckFilters(t)
	p, s := beam.NewPipelineWithRoot()
	sums := TriggerAfterProcessingTime(s)
	sums = beam.WindowInto(s, window.NewGlobalWindows(), sums)
	passert.Equals(s, sums, 6.0)
	ptest.RunAndValidate(t, p)
}

func TestTriggerAlways(t *testing.T) {
	integration.CheckFilters(t)
	p, s := beam.NewPipelineWithRoot()
	sums := TriggerAlways(s)
	sums = beam.WindowInto(s, window.NewGlobalWindows(), sums)
	passert.Equals(s, sums, 1.0, 2.0, 3.0)
	ptest.RunAndValidate(t, p)
}
