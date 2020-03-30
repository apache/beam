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

package harness

import (
	"strconv"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/metrics"
)

func TestGetShortID(t *testing.T) {
	tests := []struct {
		id           string
		urn          mUrn
		labels       metrics.Labels
		expectedUrn  string
		expectedType string
	}{
		{
			id:           "1",
			urn:          urnUserDistInt64,
			expectedUrn:  "beam:metric:user:distribution_int64:v1",
			expectedType: "beam:metrics:distribution_int64:v1",
		}, {
			id:           "2",
			urn:          urnElementCount,
			expectedUrn:  "beam:metric:element_count:v1",
			expectedType: "beam:metrics:sum_int64:v1",
		}, {
			id:           "3",
			urn:          urnProgressCompleted,
			expectedUrn:  "beam:metric:ptransform_progress:completed:v1",
			expectedType: "beam:metrics:progress:v1",
		}, {
			id:           "4",
			urn:          urnUserDistFloat64,
			expectedUrn:  "beam:metric:user:distribution_double:v1",
			expectedType: "beam:metrics:distribution_double:v1",
		}, {
			// Sentinel sanity check to validate relative ordering
			// If a new urn or type is added without the paired string
			// in the list, or vice versa, this should fail with either
			// an index out of range panic or a mismatch.
			id:           "5",
			urn:          urnTestSentinel,
			expectedUrn:  "TestingSentinelUrn",
			expectedType: "TestingSentinelType",
		}, {
			id:           "6",
			urn:          urnFinishBundle,
			expectedUrn:  "beam:metric:pardo_execution_time:finish_bundle_msecs:v1",
			expectedType: "beam:metrics:sum_int64:v1",
		}, {
			// This case and the next one validates that different labels
			// with the same urn are in fact assigned different short ids.
			id:           "7",
			urn:          urnUserSumInt64,
			labels:       metrics.UserLabels("myT", "harness", "metricNumber7"),
			expectedUrn:  "beam:metric:user:sum_int64:v1",
			expectedType: "beam:metrics:sum_int64:v1",
		}, {
			id:           "8",
			urn:          urnUserSumInt64,
			labels:       metrics.UserLabels("myT", "harness", "metricNumber8"),
			expectedUrn:  "beam:metric:user:sum_int64:v1",
			expectedType: "beam:metrics:sum_int64:v1",
		}, {
			// This validates that the same labels (as 7) but a different urn also
			// gets a different short Id. In practice, users can't do this, as
			// user metrics are unique per label set, but this isn't the layer
			// to validate that condition.
			id:           "9",
			urn:          urnUserTopNFloat64,
			labels:       metrics.UserLabels("myT", "harness", "metricNumber7"),
			expectedUrn:  "beam:metric:user:top_n_double:v1",
			expectedType: "beam:metrics:top_n_double:v1",
		}, {
			id:           "a",
			urn:          urnElementCount,
			labels:       metrics.PCollectionLabels("myPCol"),
			expectedUrn:  "beam:metric:element_count:v1",
			expectedType: "beam:metrics:sum_int64:v1",
		},
	}
	cache := newShortIDCache()
	for _, test := range tests {
		t.Run(test.id, func(t *testing.T) {
			cache.mu.Lock()
			got := cache.getShortID(test.labels, test.urn)
			if want := test.id; got != want {
				t.Errorf("shortid got %v, want %v", got, want)
			}
			cache.mu.Unlock()

			info := cache.shortIdsToInfos([]string{test.id})[test.id]

			if got, want := info.GetUrn(), test.expectedUrn; got != want {
				t.Errorf("urn got %v, want %v", got, want)
			}
			if got, want := info.GetType(), test.expectedType; got != want {
				t.Errorf("type got %v, want %v", got, want)
			}
		})
	}
	// Validate that we get the same short ids with the same cache.
	for _, test := range tests {
		t.Run("cached_"+test.id, func(t *testing.T) {
			cache.mu.Lock()
			got := cache.getShortID(test.labels, test.urn)
			if want := test.id; got != want {
				t.Errorf("shortid got %v, want %v", got, want)
			}
			cache.mu.Unlock()
		})
	}
}

// TestShortIdCache_Default validates that the default cache
// is initialized properly.
func TestShortIdCache_Default(t *testing.T) {
	defaultShortIDCache.mu.Lock()
	s := getShortID(metrics.UserLabels("this", "doesn't", "matter"), urnTestSentinel)
	defaultShortIDCache.mu.Unlock()

	info := shortIdsToInfos([]string{s})[s]
	if got, want := info.GetUrn(), "TestingSentinelUrn"; got != want {
		t.Errorf("urn got %v, want %v", got, want)
	}
	if got, want := info.GetType(), "TestingSentinelType"; got != want {
		t.Errorf("type got %v, want %v", got, want)
	}
}

func BenchmarkGetShortID(b *testing.B) {
	b.Run("new", func(b *testing.B) {
		l := metrics.UserLabels("this", "doesn't", strconv.FormatInt(-1, 36))
		last := getShortID(l, urnTestSentinel)
		for i := int64(0); i < int64(b.N); i++ {
			// Ensure it's allocated to the stack.
			l = metrics.UserLabels("this", "doesn't", strconv.FormatInt(i, 36))
			got := getShortID(l, urnTestSentinel)
			if got == last {
				b.Fatalf("short collision: at %s", got)
			}
			last = got
		}
	})
	b.Run("amortized", func(b *testing.B) {
		l := metrics.UserLabels("this", "doesn't", "matter")
		c := newShortIDCache()
		want := c.getShortID(l, urnTestSentinel)
		for i := 0; i < b.N; i++ {
			got := c.getShortID(l, urnTestSentinel)
			if got != want {
				b.Fatalf("different short ids: got %s, want %s", got, want)
			}
		}
	})
}
