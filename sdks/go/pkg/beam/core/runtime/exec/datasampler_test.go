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
	"testing"
	"time"
)

// TestDataSampler verifies that the DataSampler works correctly.
func TestDataSampler(t *testing.T) {
	tests := []struct {
		name    string
		samples []DataSample
		pids    []string
		want    int
	}{
		{
			name: "GetAllSamples",
			samples: []DataSample{
				{PCollectionID: "pid1", Element: []byte("element1"), Timestamp: time.Now()},
				{PCollectionID: "pid2", Element: []byte("element2"), Timestamp: time.Now()},
			},
			pids: []string{},
			want: 2,
		},
		{
			name: "GetSamplesForPCollections",
			samples: []DataSample{
				{PCollectionID: "pid1", Element: []byte("element1"), Timestamp: time.Now()},
				{PCollectionID: "pid2", Element: []byte("element2"), Timestamp: time.Now()},
			},
			pids: []string{"pid1"},
			want: 1,
		},
		{
			name: "GetSamplesForPCollectionsWithNoResult",
			samples: []DataSample{
				{PCollectionID: "pid1", Element: []byte("element1"), Timestamp: time.Now()},
				{PCollectionID: "pid2", Element: []byte("element2"), Timestamp: time.Now()},
			},
			pids: []string{"pid3"},
			want: 0,
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
			for i := 0; i < 5; i++ {
				samples := dataSampler.GetSamples(test.pids)
				if len(samples) == test.want {
					samplesCount = len(samples)
					break
				}
				time.Sleep(time.Second)
			}
			cancel()
			if samplesCount != test.want {
				t.Errorf("got an unexpected number of sampled elements: %v, want: %v", samplesCount, test.want)
			}
		})
	}
}
