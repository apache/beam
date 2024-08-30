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

package snippets

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
)

// [START metrics_query]

func queryMetrics(pr beam.PipelineResult, ns, n string) metrics.QueryResults {
	return pr.Metrics().Query(func(r beam.MetricResult) bool {
		return r.Namespace() == ns && r.Name() == n
	})
}

// [END metrics_query]

var runner = "prism"

// [START metrics_pipeline]

func addMetricDoFnToPipeline(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, &MyMetricsDoFn{}, input)
}

func executePipelineAndGetMetrics(ctx context.Context, p *beam.Pipeline) (metrics.QueryResults, error) {
	pr, err := beam.Run(ctx, runner, p)
	if err != nil {
		return metrics.QueryResults{}, err
	}

	// Request the metric called "counter1" in namespace called "namespace"
	ms := pr.Metrics().Query(func(r beam.MetricResult) bool {
		return r.Namespace() == "namespace" && r.Name() == "counter1"
	})

	// Print the metric value - there should be only one line because there is
	// only one metric called "counter1" in the namespace called "namespace"
	for _, c := range ms.Counters() {
		fmt.Println(c.Namespace(), "-", c.Name(), ":", c.Committed)
	}
	return ms, nil
}

type MyMetricsDoFn struct {
	counter beam.Counter
}

func init() {
	beam.RegisterType(reflect.TypeOf((*MyMetricsDoFn)(nil)))
}

func (fn *MyMetricsDoFn) Setup() {
	// While metrics can be defined in package scope or dynamically
	// it's most efficient to include them in the DoFn.
	fn.counter = beam.NewCounter("namespace", "counter1")
}

func (fn *MyMetricsDoFn) ProcessElement(ctx context.Context, v beam.V, emit func(beam.V)) {
	// count the elements
	fn.counter.Inc(ctx, 1)
	emit(v)
}

// [END metrics_pipeline]
