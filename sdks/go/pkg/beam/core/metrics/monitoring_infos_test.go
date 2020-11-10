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
	"testing"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/metricsx"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
)

func TestCounterExtraction(t *testing.T) {
	var value int64 = 15
	want := CounterResult{
		Attempted: 15,
		Committed: -1,
		Key: MetricKey{
			Step:      "main.customDoFn",
			Name:      "customCounter",
			Namespace: "customDoFn",
		}}

	payload, err := metricsx.Int64Counter(value)
	if err != nil {
		panic(err)
	}

	labels := map[string]string{
		"PTRANSFORM": "main.customDoFn",
		"NAMESPACE":  "customDoFn",
		"NAME":       "customCounter",
	}

	mInfo := &pipepb.MonitoringInfo{
		Urn:     metricsx.UrnToString(metricsx.UrnUserSumInt64),
		Type:    metricsx.UrnToType(metricsx.UrnUserSumInt64),
		Labels:  labels,
		Payload: payload,
	}

	attempted := []*pipepb.MonitoringInfo{mInfo}
	committed := []*pipepb.MonitoringInfo{}

	got, _, _ := FromMonitoringInfos(attempted, committed)
	size := len(got)
	if size < 1 {
		t.Fatalf("Invalid array's size: got: %v, expected: %v", size, 1)
	}
	if got[0] != want {
		t.Fatalf("Invalid counter: got: %v, want: %v",
			got[0], want)
	}
}

func TestDistributionExtraction(t *testing.T) {
	var count, sum, min, max int64 = 100, 5, -12, 30

	want := DistributionResult{
		Attempted: DistributionValue{
			Count: 100,
			Sum:   5,
			Min:   -12,
			Max:   30,
		},
		Committed: DistributionValue{},
		Key: MetricKey{
			Step:      "main.customDoFn",
			Name:      "customDist",
			Namespace: "customDoFn",
		}}

	payload, err := metricsx.Int64Distribution(count, sum, min, max)
	if err != nil {
		panic(err)
	}

	labels := map[string]string{
		"PTRANSFORM": "main.customDoFn",
		"NAMESPACE":  "customDoFn",
		"NAME":       "customDist",
	}

	mInfo := &pipepb.MonitoringInfo{
		Urn:     metricsx.UrnToString(metricsx.UrnUserDistInt64),
		Type:    metricsx.UrnToType(metricsx.UrnUserDistInt64),
		Labels:  labels,
		Payload: payload,
	}

	attempted := []*pipepb.MonitoringInfo{mInfo}
	committed := []*pipepb.MonitoringInfo{}

	_, got, _ := FromMonitoringInfos(attempted, committed)
	size := len(got)
	if size < 1 {
		t.Fatalf("Invalid array's size: got: %v, expected: %v", size, 1)
	}
	if got[0] != want {
		t.Fatalf("Invalid distribution: got: %v, want: %v",
			got[0], want)
	}
}

func TestGaugeExtraction(t *testing.T) {
	var value int64 = 100
	loc, _ := time.LoadLocation("Local")
	tm := time.Date(2020, 11, 9, 17, 52, 28, 462*int(time.Millisecond), loc)

	want := GaugeResult{
		Attempted: GaugeValue{
			Value:     100,
			Timestamp: tm,
		},
		Committed: GaugeValue{},
		Key: MetricKey{
			Step:      "main.customDoFn",
			Name:      "customGauge",
			Namespace: "customDoFn",
		}}

	payload, err := metricsx.Int64Latest(tm, value)
	if err != nil {
		panic(err)
	}

	labels := map[string]string{
		"PTRANSFORM": "main.customDoFn",
		"NAMESPACE":  "customDoFn",
		"NAME":       "customGauge",
	}

	mInfo := &pipepb.MonitoringInfo{
		Urn:     metricsx.UrnToString(metricsx.UrnUserLatestMsInt64),
		Type:    metricsx.UrnToType(metricsx.UrnUserLatestMsInt64),
		Labels:  labels,
		Payload: payload,
	}

	attempted := []*pipepb.MonitoringInfo{mInfo}
	committed := []*pipepb.MonitoringInfo{}

	_, _, got := FromMonitoringInfos(attempted, committed)
	size := len(got)
	if size < 1 {
		t.Fatalf("Invalid array's size: got: %v, expected: %v", size, 1)
	}
	if got[0] != want {
		t.Fatalf("Invalid gauge: got: %v, want: %v",
			got[0], want)
	}
}
