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

package dataflowlib

import (
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
	df "google.golang.org/api/dataflow/v1b3"
)

func TestFromMetricUpdates_Counters(t *testing.T) {
	want := metrics.CounterResult{
		Attempted: 15,
		Committed: 15,
		Key: metrics.StepKey{
			Step:      "main.customDoFn",
			Name:      "customCounter",
			Namespace: "customDoFn",
		}}
	cName := newMetricStructuredName("customCounter", "customDoFn", false)
	committed := df.MetricUpdate{Name: &cName, Scalar: 15.0}

	aName := newMetricStructuredName("customCounter", "customDoFn", true)
	attempted := df.MetricUpdate{Name: &aName, Scalar: 15.0}

	p, err := newPipeline("main.customDoFn")
	if err != nil {
		t.Fatal(err)
	}

	got := FromMetricUpdates([]*df.MetricUpdate{&attempted, &committed}, p).AllMetrics().Counters()
	size := len(got)
	if size < 1 {
		t.Fatalf("Invalid array's size: got: %v, want: %v", size, 1)
	}
	if d := cmp.Diff(want, got[0]); d != "" {
		t.Fatalf("Invalid counter: got: %v, want: %v, diff(-want,+got):\n %v",
			got[0], want, d)
	}
}

func TestFromMetricUpdates_Distributions(t *testing.T) {
	want := metrics.DistributionResult{
		Attempted: metrics.DistributionValue{
			Count: 100,
			Sum:   5,
			Min:   -12,
			Max:   30,
		},
		Committed: metrics.DistributionValue{
			Count: 100,
			Sum:   5,
			Min:   -12,
			Max:   30,
		},
		Key: metrics.StepKey{
			Step:      "main.customDoFn",
			Name:      "customDist",
			Namespace: "customDoFn",
		}}
	distribution := map[string]any{
		"count": 100.0,
		"sum":   5.0,
		"min":   -12.0,
		"max":   30.0,
	}
	cName := newMetricStructuredName("customDist", "customDoFn", false)
	committed := df.MetricUpdate{Name: &cName, Distribution: distribution}

	aName := newMetricStructuredName("customDist", "customDoFn", true)
	attempted := df.MetricUpdate{Name: &aName, Distribution: distribution}

	p, err := newPipeline("main.customDoFn")
	if err != nil {
		t.Fatal(err)
	}

	got := FromMetricUpdates([]*df.MetricUpdate{&attempted, &committed}, p).AllMetrics().Distributions()
	size := len(got)
	if size < 1 {
		t.Fatalf("Invalid array's size: got: %v, want: %v", size, 1)
	}
	if d := cmp.Diff(want, got[0]); d != "" {
		t.Fatalf("Invalid distribution: got: %v, want: %v, diff(-want,+got):\n %v",
			got[0], want, d)
	}
}

func newMetricStructuredName(name, namespace string, attempted bool) df.MetricStructuredName {
	context := map[string]string{
		"step":      "e5",
		"namespace": namespace,
	}
	if attempted {
		context["tentative"] = "true"
	}
	return df.MetricStructuredName{Context: context, Name: name}
}

func newPipeline(stepName string) (*pipepb.Pipeline, error) {
	p := &pipepb.Pipeline{
		Components: &pipepb.Components{
			Transforms: map[string]*pipepb.PTransform{
				"e5": {
					UniqueName: stepName,
				},
			},
		},
	}
	return p, nil
}
