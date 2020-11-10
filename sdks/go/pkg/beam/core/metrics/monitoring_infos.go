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

package metrics

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
)

// FromMonitoringInfos extracts metrics from GetJobMetrics's response and
// groups them into counters, distributions and gauges.
func FromMonitoringInfos(attempted []*pipepb.MonitoringInfo, committed []*pipepb.MonitoringInfo) (
	[]CounterResult,
	[]DistributionResult,
	[]GaugeResult) {
	ac, ad, ag := groupByType(attempted)
	cc, cd, cg := groupByType(committed)

	c := mergeCounters(ac, cc)
	d := mergeDistributions(ad, cd)
	g := mergeGauges(ag, cg)

	return c, d, g
}

// IsCounter returns true if the monitoring info is a counter metric.
func IsCounter(mi *pipepb.MonitoringInfo) bool {
	return mi.GetType() == "beam:metrics:sum_int64:v1"
}

// IsDistribution returns true if the monitoring info is a distribution metric.
func IsDistribution(mi *pipepb.MonitoringInfo) bool {
	return mi.GetType() == "beam:metrics:distribution_int64:v1"
}

// IsGauge returns true if the monitoring info is a gauge metric.
func IsGauge(mi *pipepb.MonitoringInfo) bool {
	switch mi.GetType() {
	case
		"beam:metrics:latest_int64:v1",
		"beam:metrics:top_n_int64:v1",
		"beam:metrics:bottom_n_int64:v1":
		return true
	}
	return false
}

func groupByType(minfos []*pipepb.MonitoringInfo) (
	map[MetricKey]int64,
	map[MetricKey]DistributionValue,
	map[MetricKey]GaugeValue) {
	counters := make(map[MetricKey]int64)
	distributions := make(map[MetricKey]DistributionValue)
	gauges := make(map[MetricKey]GaugeValue)

	for _, minfo := range minfos {
		key, err := extractKey(minfo)
		if err != nil {
			log.Println(err)
			continue
		}

		r := bytes.NewReader(minfo.GetPayload())

		if IsCounter(minfo) {
			value, err := extractCounterValue(r)
			if err != nil {
				log.Println(err)
				continue
			}
			counters[key] = value
		} else if IsDistribution(minfo) {
			value, err := extractDistributionValue(r)
			if err != nil {
				log.Println(err)
				continue
			}
			distributions[key] = value
		} else if IsGauge(minfo) {
			value, err := extractGaugeValue(r)
			if err != nil {
				log.Println(err)
				continue
			}
			gauges[key] = value
		} else {
			log.Println("unknown metric type")
		}
	}
	return counters, distributions, gauges
}

func mergeCounters(attempted map[MetricKey]int64, committed map[MetricKey]int64) []CounterResult {
	res := make([]CounterResult, 0)

	for k := range attempted {
		v, ok := committed[k]
		if !ok {
			v = -1
		}
		res = append(res, CounterResult{Attempted: attempted[k], Committed: v, Key: k})
	}
	return res
}

func mergeDistributions(attempted map[MetricKey]DistributionValue, committed map[MetricKey]DistributionValue) []DistributionResult {
	res := make([]DistributionResult, 0)

	for k := range attempted {
		v, ok := committed[k]
		if !ok {
			v = DistributionValue{}
		}
		res = append(res, DistributionResult{Attempted: attempted[k], Committed: v, Key: k})
	}
	return res
}

func mergeGauges(attempted map[MetricKey]GaugeValue, committed map[MetricKey]GaugeValue) []GaugeResult {
	res := make([]GaugeResult, 0)

	for k := range attempted {
		v, ok := committed[k]
		if !ok {
			v = GaugeValue{}
		}
		res = append(res, GaugeResult{Attempted: attempted[k], Committed: v, Key: k})
	}
	return res
}

func extractKey(mi *pipepb.MonitoringInfo) (MetricKey, error) {
	labels := newLabels(mi.GetLabels())
	stepName := getStepName(labels)
	if stepName == "" {
		return MetricKey{}, fmt.Errorf("Failed to deduce Step from MonitoringInfo: %v", mi)
	}
	return MetricKey{stepName, labels.Name(), labels.Namespace()}, nil
}

func extractCounterValue(reader *bytes.Reader) (int64, error) {
	value, err := coder.DecodeVarInt(reader)
	if err != nil {
		return -1, err
	}
	return value, nil
}

func extractDistributionValue(reader *bytes.Reader) (DistributionValue, error) {
	values, err := decodeMany(reader, 4)
	if err != nil {
		return DistributionValue{}, err
	}
	return DistributionValue{Count: values[0], Sum: values[1], Min: values[2], Max: values[3]}, nil
}

func extractGaugeValue(reader *bytes.Reader) (GaugeValue, error) {
	values, err := decodeMany(reader, 2)
	if err != nil {
		return GaugeValue{}, err
	}
	return GaugeValue{Timestamp: time.Unix(0, values[0]*int64(time.Millisecond)), Value: values[1]}, nil
}

func newLabels(miLabels map[string]string) *Labels {
	return &Labels{
		transform:   miLabels["PTRANSFORM"],
		namespace:   miLabels[pipepb.MonitoringInfo_NAMESPACE.String()],
		name:        miLabels[pipepb.MonitoringInfo_NAME.String()],
		pcollection: miLabels[pipepb.MonitoringInfo_PCOLLECTION.String()]}
}

func getStepName(labels *Labels) string {
	return labels.Transform()
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
