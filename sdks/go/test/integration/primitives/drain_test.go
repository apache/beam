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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

func TestDrain(t *testing.T) {
	integration.CheckFilters(t)

	p, s := beam.NewPipelineWithRoot()
	Drain(s)
	pr, err := ptest.RunWithMetrics(p)
	if err != nil {
		t.Errorf("Drain test failed: %v", err)
	}

	qr := pr.Metrics().Query(func(mr beam.MetricResult) bool {
		return mr.Name() == "truncate"
	})
	counter := metrics.CounterResult{}
	if len(qr.Counters()) != 0 {
		counter = qr.Counters()[0]
	}
	if counter.Result() != 1 {
		t.Errorf("Metrics().Query(by Name) failed. Got %d counters, Want %d counters", counter.Result(), 1)
	}
}
