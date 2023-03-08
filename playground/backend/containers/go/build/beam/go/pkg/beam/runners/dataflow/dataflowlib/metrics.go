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
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	df "google.golang.org/api/dataflow/v1b3"
)

// FromMetricUpdates extracts metrics from a slice of MetricUpdate objects and
// groups them into counters, distributions and gauges.
//
// Dataflow currently only reports Counter and Distribution metrics to Cloud
// Monitoring. Gauge metrics are not supported. The output metrics.Results will
// not contain any gauges.
func FromMetricUpdates(allMetrics []*df.MetricUpdate, p *pipepb.Pipeline) *metrics.Results {
	ac, ad := groupByType(allMetrics, p, true)
	cc, cd := groupByType(allMetrics, p, false)

	return metrics.NewResults(metrics.MergeCounters(ac, cc), metrics.MergeDistributions(ad, cd), make([]metrics.GaugeResult, 0), make([]metrics.MsecResult, 0), make([]metrics.PColResult, 0))
}

func groupByType(allMetrics []*df.MetricUpdate, p *pipepb.Pipeline, tentative bool) (
	map[metrics.StepKey]int64,
	map[metrics.StepKey]metrics.DistributionValue) {
	counters := make(map[metrics.StepKey]int64)
	distributions := make(map[metrics.StepKey]metrics.DistributionValue)

	for _, metric := range allMetrics {
		isTentative := metric.Name.Context["tentative"] == "true"
		if isTentative != tentative {
			continue
		}

		key, err := extractKey(metric, p)
		if err != nil {
			continue
		}

		if metric.Scalar != nil {
			v, err := extractCounterValue(metric.Scalar)
			if err != nil {
				continue
			}
			counters[key] = v
		} else if metric.Distribution != nil {
			v, err := extractDistributionValue(metric.Distribution)
			if err != nil {
				continue
			}
			distributions[key] = v
		}
	}
	return counters, distributions
}

func extractKey(metric *df.MetricUpdate, p *pipepb.Pipeline) (metrics.StepKey, error) {
	stepName, ok := metric.Name.Context["step"]
	if !ok {
		return metrics.StepKey{}, fmt.Errorf("could not find the internal step name")
	}
	userStepName := ""

	for k, transform := range p.GetComponents().GetTransforms() {
		if k == stepName {
			userStepName = transform.GetUniqueName()
			break
		}
	}
	if userStepName == "" {
		return metrics.StepKey{}, fmt.Errorf("could not translate the internal step name %v", stepName)
	}

	namespace := metric.Name.Context["namespace"]
	if namespace == "" {
		namespace = "dataflow/v1b3"
	}

	return metrics.StepKey{Step: userStepName, Name: metric.Name.Name, Namespace: namespace}, nil
}

func extractCounterValue(obj any) (int64, error) {
	v, ok := obj.(float64)
	if !ok {
		return -1, fmt.Errorf("expected float64, got data of type %T instead", obj)
	}
	return int64(v), nil
}

func extractDistributionValue(obj any) (metrics.DistributionValue, error) {
	m := obj.(map[string]any)
	propertiesToVisit := []string{"count", "sum", "min", "max"}
	var values [4]int64

	for i, p := range propertiesToVisit {
		v, ok := m[p].(float64)
		if !ok {
			return metrics.DistributionValue{}, fmt.Errorf("expected float64, got data of type %T instead", m[p])
		}
		values[i] = int64(v)
	}
	return metrics.DistributionValue{Count: values[0], Sum: values[1], Min: values[2], Max: values[3]}, nil
}
