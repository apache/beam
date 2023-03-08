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

package regression

import (
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"

	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/samza"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/spark"
)

func TestLPErrorPipeline(t *testing.T) {
	integration.CheckFilters(t)

	pipeline, s := beam.NewPipelineWithRoot()
	want := beam.CreateList(s, []int{0})
	got := LPErrorPipeline(s)
	passert.Equals(s, got, want)

	ptest.RunAndValidate(t, pipeline)
}

func TestLPErrorReshufflePipeline(t *testing.T) {
	integration.CheckFilters(t)

	pipeline, s := beam.NewPipelineWithRoot()
	LPErrorReshufflePipeline(s)

	ptest.RunAndValidate(t, pipeline)
}

func TestLPErrorReshufflePipeline_PAssert(t *testing.T) {
	integration.CheckFilters(t)

	pipeline, s := beam.NewPipelineWithRoot()
	got := LPErrorReshufflePipeline(s)
	passert.Equals(s, got, fruit{"Apple"}, fruit{"Banana"}, fruit{"Cherry"})

	ptest.RunAndValidate(t, pipeline)
}

func TestLPErrorReshufflePipeline_SideInput(t *testing.T) {
	integration.CheckFilters(t)

	pipeline, s := beam.NewPipelineWithRoot()
	got := LPErrorReshufflePipeline(s)
	beam.ParDo0(s, &iterSideStrings{
		Wants: []string{"Apple", "Banana", "Cherry"},
	}, beam.Impulse(s), beam.SideInput{Input: got})

	ptest.RunAndValidate(t, pipeline)
}

func checkFruitCount(t *testing.T, pr beam.PipelineResult) {
	t.Helper()
	fcr := pr.Metrics().Query(func(sr metrics.SingleResult) bool {
		return sr.Namespace() == MetricNamespace && sr.Name() == FruitCounterName
	})
	if len(fcr.Counters()) == 0 {
		t.Logf("no counters found: check if %v supports counters", *ptest.Runner)
		return
	}
	if got, want := fcr.Counters()[0].Result(), int64(3); got != want {
		t.Errorf("unexpected fruit count: got %v, want %v", got, want)
	}
}

func TestLPErrorReshufflePipeline_DoFn(t *testing.T) {
	integration.CheckFilters(t)

	pipeline, s := beam.NewPipelineWithRoot()
	got := LPErrorReshufflePipeline(s)
	beam.ParDo(s, countFruit, got)

	pr := ptest.RunAndValidate(t, pipeline)
	checkFruitCount(t, pr)
}

func TestLPErrorReshufflePipeline_DoFnPAssert(t *testing.T) {
	integration.CheckFilters(t)

	pipeline, s := beam.NewPipelineWithRoot()
	got := LPErrorReshufflePipeline(s)
	counted := beam.ParDo(s, countFruit, got)
	passert.Equals(s, counted, fruit{"Apple"}, fruit{"Banana"}, fruit{"Cherry"})

	pr := ptest.RunAndValidate(t, pipeline)
	checkFruitCount(t, pr)
}

func TestLPErrorReshufflePipeline_DoFnSideInput(t *testing.T) {
	integration.CheckFilters(t)

	pipeline, s := beam.NewPipelineWithRoot()
	got := LPErrorReshufflePipeline(s)
	counted := beam.ParDo(s, countFruit, got)
	beam.ParDo0(s, &iterSideStrings{
		Wants: []string{"Apple", "Banana", "Cherry"},
	}, beam.Impulse(s), beam.SideInput{Input: counted})

	pr := ptest.RunAndValidate(t, pipeline)
	checkFruitCount(t, pr)
}
