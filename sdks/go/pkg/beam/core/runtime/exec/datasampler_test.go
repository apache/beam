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

package exec

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"
)

// TestDataSampler verifies that the DataSampler works correctly.
func TestDataSampler(t *testing.T) {
	timestamp := time.Now()
	tests := []struct {
		name    string
		samples []DataSample
		pids    []string
		want    map[string][]*DataSample
	}{
		{
			name: "GetAllSamples",
			samples: []DataSample{
				{PCollectionID: "pid1", Element: []byte("element1"), Timestamp: timestamp},
				{PCollectionID: "pid2", Element: []byte("element2"), Timestamp: timestamp},
			},
			pids: []string{},
			want: map[string][]*DataSample{
				"pid1": {{PCollectionID: "pid1", Element: []byte("element1"), Timestamp: timestamp}},
				"pid2": {{PCollectionID: "pid2", Element: []byte("element2"), Timestamp: timestamp}},
			},
		},
		{
			name: "GetSamplesForPCollections",
			samples: []DataSample{
				{PCollectionID: "pid1", Element: []byte("element1"), Timestamp: timestamp},
				{PCollectionID: "pid2", Element: []byte("element2"), Timestamp: timestamp},
			},
			pids: []string{"pid1"},
			want: map[string][]*DataSample{
				"pid1": {{PCollectionID: "pid1", Element: []byte("element1"), Timestamp: timestamp}},
			},
		},
		{
			name: "GetSamplesForPCollectionsWithNoResult",
			samples: []DataSample{
				{PCollectionID: "pid1", Element: []byte("element1"), Timestamp: timestamp},
				{PCollectionID: "pid2", Element: []byte("element2"), Timestamp: timestamp},
			},
			pids: []string{"pid3"},
			want: map[string][]*DataSample{},
		},
		{
			name: "GetSamplesForPCollectionsTooManySamples",
			samples: []DataSample{
				{PCollectionID: "pid1", Element: []byte("element1"), Timestamp: timestamp},
				{PCollectionID: "pid1", Element: []byte("element2"), Timestamp: timestamp},
				{PCollectionID: "pid1", Element: []byte("element3"), Timestamp: timestamp},
				{PCollectionID: "pid1", Element: []byte("element4"), Timestamp: timestamp},
				{PCollectionID: "pid1", Element: []byte("element5"), Timestamp: timestamp},
				{PCollectionID: "pid1", Element: []byte("element6"), Timestamp: timestamp},
				{PCollectionID: "pid1", Element: []byte("element7"), Timestamp: timestamp},
				{PCollectionID: "pid1", Element: []byte("element8"), Timestamp: timestamp},
				{PCollectionID: "pid1", Element: []byte("element9"), Timestamp: timestamp},
				{PCollectionID: "pid1", Element: []byte("element10"), Timestamp: timestamp},
				{PCollectionID: "pid1", Element: []byte("element11"), Timestamp: timestamp},
			},
			pids: []string{"pid1"},
			want: map[string][]*DataSample{
				"pid1": {
					{PCollectionID: "pid1", Element: []byte("element2"), Timestamp: timestamp},
					{PCollectionID: "pid1", Element: []byte("element3"), Timestamp: timestamp},
					{PCollectionID: "pid1", Element: []byte("element4"), Timestamp: timestamp},
					{PCollectionID: "pid1", Element: []byte("element5"), Timestamp: timestamp},
					{PCollectionID: "pid1", Element: []byte("element6"), Timestamp: timestamp},
					{PCollectionID: "pid1", Element: []byte("element7"), Timestamp: timestamp},
					{PCollectionID: "pid1", Element: []byte("element8"), Timestamp: timestamp},
					{PCollectionID: "pid1", Element: []byte("element9"), Timestamp: timestamp},
					{PCollectionID: "pid1", Element: []byte("element10"), Timestamp: timestamp},
					{PCollectionID: "pid1", Element: []byte("element11"), Timestamp: timestamp},
				}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			dataSampler := NewDataSampler(ctx)
			go dataSampler.Process()
			for _, sample := range test.samples {
				dataSampler.SendSample(sample.PCollectionID, sample.Element, sample.Timestamp)
			}
			var samplesCount = -1
			var samples map[string][]*DataSample
			for i := 0; i < 5; i++ {
				samples = dataSampler.GetSamples(test.pids)
				if len(samples) == len(test.want) {
					samplesCount = len(samples)
					break
				}
				time.Sleep(time.Second)
			}
			cancel()
			if samplesCount != len(test.want) {
				t.Errorf("got an unexpected number of sampled elements: %v, want: %v", samplesCount, len(test.want))
			}
			if !verifySampledElements(samples, test.want) {
				t.Errorf("got an unexpected sampled elements: %v, want: %v", samples, test.want)
			}
		})
	}
}

func verifySampledElements(samples, want map[string][]*DataSample) bool {
	if len(samples) != len(want) {
		return false
	}
	for pid, samples := range samples {
		expected, ok := want[pid]
		if !ok {
			return false
		}
		sort.SliceStable(samples, func(i, j int) bool {
			return string(samples[i].Element) < string(samples[j].Element)
		})
		sort.SliceStable(expected, func(i, j int) bool {
			return string(expected[i].Element) < string(expected[j].Element)
		})
		if !reflect.DeepEqual(samples, expected) {
			return false
		}
	}
	return true
}
