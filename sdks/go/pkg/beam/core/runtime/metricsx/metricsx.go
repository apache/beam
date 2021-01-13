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

package metricsx

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/metrics"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
)

// FromMonitoringInfos extracts metrics from monitored states and
// groups them into counters, distributions and gauges.
func FromMonitoringInfos(attempted []*pipepb.MonitoringInfo, committed []*pipepb.MonitoringInfo) *metrics.Results {
	ac, ad, ag := groupByType(attempted)
	cc, cd, cg := groupByType(committed)

	return metrics.NewResults(metrics.MergeCounters(ac, cc), metrics.MergeDistributions(ad, cd), metrics.MergeGauges(ag, cg))
}

func groupByType(minfos []*pipepb.MonitoringInfo) (
	map[metrics.StepKey]int64,
	map[metrics.StepKey]metrics.DistributionValue,
	map[metrics.StepKey]metrics.GaugeValue) {
	counters := make(map[metrics.StepKey]int64)
	distributions := make(map[metrics.StepKey]metrics.DistributionValue)
	gauges := make(map[metrics.StepKey]metrics.GaugeValue)

	for _, minfo := range minfos {
		key, err := extractKey(minfo)
		if err != nil {
			log.Println(err)
			continue
		}

		r := bytes.NewReader(minfo.GetPayload())

		switch minfo.GetType() {
		case "beam:metrics:sum_int64:v1":
			value, err := extractCounterValue(r)
			if err != nil {
				log.Println(err)
				continue
			}
			counters[key] = value
		case "beam:metrics:distribution_int64:v1":
			value, err := extractDistributionValue(r)
			if err != nil {
				log.Println(err)
				continue
			}
			distributions[key] = value
		case
			"beam:metrics:latest_int64:v1",
			"beam:metrics:top_n_int64:v1",
			"beam:metrics:bottom_n_int64:v1":
			value, err := extractGaugeValue(r)
			if err != nil {
				log.Println(err)
				continue
			}
			gauges[key] = value
		default:
			log.Println("unknown metric type")
		}
	}
	return counters, distributions, gauges
}

func extractKey(mi *pipepb.MonitoringInfo) (metrics.StepKey, error) {
	labels := newLabels(mi.GetLabels())
	stepName := labels.Transform()
	if stepName == "" {
		return metrics.StepKey{}, fmt.Errorf("Failed to deduce Step from MonitoringInfo: %v", mi)
	}
	return metrics.StepKey{Step: stepName, Name: labels.Name(), Namespace: labels.Namespace()}, nil
}

func extractCounterValue(reader *bytes.Reader) (int64, error) {
	value, err := coder.DecodeVarInt(reader)
	if err != nil {
		return -1, err
	}
	return value, nil
}

func extractDistributionValue(reader *bytes.Reader) (metrics.DistributionValue, error) {
	values, err := decodeMany(reader, 4)
	if err != nil {
		return metrics.DistributionValue{}, err
	}
	return metrics.DistributionValue{Count: values[0], Sum: values[1], Min: values[2], Max: values[3]}, nil
}

func extractGaugeValue(reader *bytes.Reader) (metrics.GaugeValue, error) {
	values, err := decodeMany(reader, 2)
	if err != nil {
		return metrics.GaugeValue{}, err
	}
	return metrics.GaugeValue{Timestamp: time.Unix(0, values[0]*int64(time.Millisecond)), Value: values[1]}, nil
}

func newLabels(miLabels map[string]string) *metrics.Labels {
	labels := metrics.UserLabels(miLabels["PTRANSFORM"], miLabels["NAMESPACE"], miLabels["NAME"])
	return &labels
}

func decodeMany(reader *bytes.Reader, size int) ([]int64, error) {
	var err error
	values := make([]int64, size)

	for i := 0; i < size; i++ {
		values[i], err = coder.DecodeVarInt(reader)
		if err != nil {
			return nil, err
		}
	}
	return values, err
}
