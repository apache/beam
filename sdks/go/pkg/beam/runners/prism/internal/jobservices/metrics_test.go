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

package jobservices

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/testing/protocmp"
)

var metSpecs = (pipepb.MonitoringInfoSpecs_Enum)(0).Descriptor().Values()

// makeInfo generates dummy Monitoring infos from a spec.
func makeInfo(enum pipepb.MonitoringInfoSpecs_Enum) *pipepb.MonitoringInfo {
	spec := proto.GetExtension(metSpecs.ByNumber(protoreflect.EnumNumber(enum)).Options(), pipepb.E_MonitoringInfoSpec).(*pipepb.MonitoringInfoSpec)

	labels := map[string]string{}
	for _, l := range spec.GetRequiredLabels() {
		labels[l] = l
	}
	return &pipepb.MonitoringInfo{
		Urn:    spec.GetUrn(),
		Type:   spec.GetType(),
		Labels: labels,
	}
}

func makeInfoWBytes(enum pipepb.MonitoringInfoSpecs_Enum, payload []byte) *pipepb.MonitoringInfo {
	info := makeInfo(enum)
	info.Payload = payload
	return info
}

// This test validates that multiple contributions are correctly summed up and accumulated.
func Test_metricsStore_ContributeMetrics(t *testing.T) {

	doubleBytes := func(v float64) []byte {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, math.Float64bits(v))
		return b
	}

	progress := func(vs ...float64) []byte {
		var buf bytes.Buffer
		coder.EncodeInt32(int32(len(vs)), &buf)
		for _, v := range vs {
			coder.EncodeDouble(v, &buf)
		}
		return buf.Bytes()
	}

	tests := []struct {
		name string

		// TODO convert input to non-legacy metrics once we support, and then delete these.
		input    []map[string][]byte
		shortIDs map[string]*pipepb.MonitoringInfo

		want []*pipepb.MonitoringInfo
	}{
		{
			name: "int64Sum",
			input: []map[string][]byte{
				{"a": []byte{3}},
				{"a": []byte{5}},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_USER_SUM_INT64),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_SUM_INT64, []byte{8}),
			},
		}, {
			name: "float64Sum",
			input: []map[string][]byte{
				{"a": doubleBytes(3.14)},
				{"a": doubleBytes(1.06)},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_USER_SUM_DOUBLE),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_SUM_DOUBLE, doubleBytes(4.20)),
			},
		}, {
			name: "progress",
			input: []map[string][]byte{
				{"a": progress(1, 2.2, 78)},
				{"a": progress(0, 7.8, 22)},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_WORK_REMAINING),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_WORK_REMAINING, progress(0, 7.8, 22)),
			},
		}, {
			name: "int64Distribution",
			input: []map[string][]byte{
				{"a": []byte{1, 2, 2, 2}},
				{"a": []byte{3, 17, 5, 7}},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_USER_DISTRIBUTION_INT64),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_DISTRIBUTION_INT64, []byte{4, 19, 2, 7}),
			},
		}, {
			name: "int64Gauge",
			input: []map[string][]byte{
				{"a": []byte{3, 5}},
				{"a": []byte{14, 2}},
				{"a": []byte{10, 18}},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_USER_LATEST_INT64),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_LATEST_INT64, []byte{14, 2}),
			},
		}, {
			name: "float64Gauge",
			input: []map[string][]byte{
				{"a": append([]byte{2}, doubleBytes(45)...)},
				{"a": append([]byte{17}, doubleBytes(2)...)},
				{"a": append([]byte{16}, doubleBytes(200)...)},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_USER_LATEST_DOUBLE),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_LATEST_DOUBLE, append([]byte{17}, doubleBytes(2)...)),
			},
		}, {
			name: "stringSet",
			input: []map[string][]byte{
				{"a": []byte{0, 0, 0, 1, 1, 63}},
				{"a": []byte{0, 0, 0, 2, 1, 63, 1, 63}},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_USER_SET_STRING),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_SET_STRING, []byte{0, 0, 0, 1, 1, 63}),
			},
		}, {
			name: "boundedTrieSingleton",
			input: []map[string][]byte{
				{"a": mustMarshal(t, &pipepb.BoundedTrie{Singleton: []string{"root", "seg"}})},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_USER_BOUNDED_TRIE),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_BOUNDED_TRIE, mustMarshal(t, &pipepb.BoundedTrie{
					Bound:     100,
					Singleton: []string{"root", "seg"},
				})),
			},
		}, {
			name: "boundedTrieMergeSingletons",
			input: []map[string][]byte{
				{"a": mustMarshal(t, &pipepb.BoundedTrie{Bound: 100, Singleton: []string{"root", "a"}})},
				{"a": mustMarshal(t, &pipepb.BoundedTrie{Bound: 100, Singleton: []string{"root", "b"}})},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_USER_BOUNDED_TRIE),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_BOUNDED_TRIE, mustMarshal(t, &pipepb.BoundedTrie{
					Bound: 100,
					Root: &pipepb.BoundedTrieNode{
						Children: map[string]*pipepb.BoundedTrieNode{
							"root": {
								Children: map[string]*pipepb.BoundedTrieNode{
									"a": {},
									"b": {},
								},
							},
						},
					},
				})),
			},
		}, {
			name: "boundedTrieMergeRoots",
			input: []map[string][]byte{
				{"a": mustMarshal(t, &pipepb.BoundedTrie{
					Bound: 100,
					Root: &pipepb.BoundedTrieNode{
						Children: map[string]*pipepb.BoundedTrieNode{
							"a": {Children: map[string]*pipepb.BoundedTrieNode{"b": {}}},
							"d": {},
						},
					},
				})},
				{"a": mustMarshal(t, &pipepb.BoundedTrie{
					Bound: 80,
					Root: &pipepb.BoundedTrieNode{
						Children: map[string]*pipepb.BoundedTrieNode{
							"a": {Children: map[string]*pipepb.BoundedTrieNode{"c": {}}},
							"e": {Children: map[string]*pipepb.BoundedTrieNode{"f": {}}},
						},
					},
				})},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_USER_BOUNDED_TRIE),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_BOUNDED_TRIE, mustMarshal(t, &pipepb.BoundedTrie{
					Bound: 80,
					Root: &pipepb.BoundedTrieNode{
						Children: map[string]*pipepb.BoundedTrieNode{
							"a": {Children: map[string]*pipepb.BoundedTrieNode{"b": {}, "c": {}}},
							"d": {},
							"e": {Children: map[string]*pipepb.BoundedTrieNode{"f": {}}},
						},
					},
				})),
			},
		}, {
			name: "boundedTrieTruncated",
			input: []map[string][]byte{
				{"a": mustMarshal(t, &pipepb.BoundedTrie{
					Bound: 100,
					Root: &pipepb.BoundedTrieNode{
						Children: map[string]*pipepb.BoundedTrieNode{
							"a": {Truncated: true},
							"b": {Children: map[string]*pipepb.BoundedTrieNode{"x": {}, "y": {}}},
						},
					},
				})},
				{"a": mustMarshal(t, &pipepb.BoundedTrie{
					Bound: 100,
					Root: &pipepb.BoundedTrieNode{
						Children: map[string]*pipepb.BoundedTrieNode{
							"a": {Children: map[string]*pipepb.BoundedTrieNode{"z": {}}},
							"b": {Truncated: true},
							"c": {},
						},
					},
				})},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_USER_BOUNDED_TRIE),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_BOUNDED_TRIE, mustMarshal(t, &pipepb.BoundedTrie{
					Bound: 100,
					Root: &pipepb.BoundedTrieNode{
						Children: map[string]*pipepb.BoundedTrieNode{
							"a": {Truncated: true},
							"b": {Truncated: true},
							"c": {},
						},
					},
				})),
			},
		}, {
			name: "boundedTrieTrim",
			input: []map[string][]byte{
				{"a": mustMarshal(t, &pipepb.BoundedTrie{
					Bound: 2,
					Root: &pipepb.BoundedTrieNode{
						Children: map[string]*pipepb.BoundedTrieNode{
							"a": {Children: map[string]*pipepb.BoundedTrieNode{"x": {}, "y": {}}},
						},
					},
				})},
				{"a": mustMarshal(t, &pipepb.BoundedTrie{
					Bound:     2,
					Singleton: []string{"b", "z"},
				})},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_USER_BOUNDED_TRIE),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_BOUNDED_TRIE, mustMarshal(t, &pipepb.BoundedTrie{
					Bound: 2,
					Root: &pipepb.BoundedTrieNode{
						Children: map[string]*pipepb.BoundedTrieNode{
							"a": {Truncated: true},
							"b": {Children: map[string]*pipepb.BoundedTrieNode{"z": {}}},
						},
					},
				})),
			},
		}, {
			name: "histogramLinear",
			input: []map[string][]byte{
				{"a": mustMarshal(t, &pipepb.HistogramValue{
					Count:         proto.Int64(3),
					BucketOptions: linearBuckets(0, 10, 4),
					BucketCounts:  []int64{1, 2},
				})},
				{"a": mustMarshal(t, &pipepb.HistogramValue{
					Count:         proto.Int64(4),
					BucketOptions: linearBuckets(0, 10, 4),
					BucketCounts:  []int64{0, 1, 2, 1},
				})},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_USER_HISTOGRAM),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_HISTOGRAM, mustMarshal(t, &pipepb.HistogramValue{
					Count:         proto.Int64(7),
					BucketOptions: linearBuckets(0, 10, 4),
					BucketCounts:  []int64{1, 3, 2, 1},
				})),
			},
		}, {
			name: "histogramExponential",
			input: []map[string][]byte{
				{"a": mustMarshal(t, &pipepb.HistogramValue{
					Count:         proto.Int64(2),
					BucketOptions: exponentialBuckets(1, 3),
					BucketCounts:  []int64{1, 1, 0},
				})},
				{"a": mustMarshal(t, &pipepb.HistogramValue{
					Count:         proto.Int64(5),
					BucketOptions: exponentialBuckets(1, 3),
					BucketCounts:  []int64{2, 0, 3},
				})},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_API_REQUEST_LATENCIES),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_API_REQUEST_LATENCIES, mustMarshal(t, &pipepb.HistogramValue{
					Count:         proto.Int64(7),
					BucketOptions: exponentialBuckets(1, 3),
					BucketCounts:  []int64{3, 1, 3},
				})),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ms := metricsStore{}

			ms.AddShortIDs(&fnpb.MonitoringInfosMetadataResponse{
				MonitoringInfo: test.shortIDs,
			})

			for _, payload := range test.input {
				ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
					MonitoringData: payload,
				})
			}

			got := ms.Results(committed)

			if diff := cmp.Diff(test.want, got, protocmp.Transform()); diff != "" {
				t.Fatalf("metricsStore.ContributeMetrics(%v) diff (-want,+got):\n%v", test.input, diff)
			}
		})
	}
}

func Test_metricsStore_UnresolvedCommittedPayloads(t *testing.T) {
	ms := metricsStore{}

	for i := range 2 {
		unknown := ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
			MonitoringData: map[string][]byte{"c1": {8}},
		})
		if diff := cmp.Diff([]string{"c1"}, unknown); diff != "" {
			t.Fatalf("call %d unknownIDs diff (-want,+got):\n%s", i, diff)
		}
	}

	ms.AddShortIDs(&fnpb.MonitoringInfosMetadataResponse{
		MonitoringInfo: map[string]*pipepb.MonitoringInfo{
			"c1": makeInfo(pipepb.MonitoringInfoSpecs_USER_SUM_INT64),
		},
	})

	want := []*pipepb.MonitoringInfo{
		makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_SUM_INT64, []byte{16}),
	}
	if diff := cmp.Diff(want, ms.Results(committed), protocmp.Transform()); diff != "" {
		t.Fatalf("Results(committed) diff (-want,+got):\n%s", diff)
	}
}

func Test_metricsStore_UnresolvedTentativePayloads(t *testing.T) {
	ms := metricsStore{}

	for i, val := range []byte{5, 12} {
		_, unknown := ms.ContributeTentativeMetrics(&fnpb.ProcessBundleProgressResponse{
			MonitoringData: map[string][]byte{"t1": {val}},
		})
		if diff := cmp.Diff([]string{"t1"}, unknown); diff != "" {
			t.Fatalf("call %d unknownIDs diff (-want,+got):\n%s", i, diff)
		}
	}

	ms.AddShortIDs(&fnpb.MonitoringInfosMetadataResponse{
		MonitoringInfo: map[string]*pipepb.MonitoringInfo{
			"t1": makeInfo(pipepb.MonitoringInfoSpecs_USER_SUM_INT64),
		},
	})

	want := []*pipepb.MonitoringInfo{
		makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_SUM_INT64, []byte{12}),
	}
	if diff := cmp.Diff(want, ms.Results(tentative), protocmp.Transform()); diff != "" {
		t.Fatalf("Results(tentative) diff (-want,+got):\n%s", diff)
	}
}

func Test_metricsStore_DuplicateAddShortIDs(t *testing.T) {
	t.Run("sameShortID", func(t *testing.T) {
		ms := metricsStore{}
		md := &fnpb.MonitoringInfosMetadataResponse{
			MonitoringInfo: map[string]*pipepb.MonitoringInfo{
				"c1": makeInfo(pipepb.MonitoringInfoSpecs_USER_SUM_INT64),
			},
		}

		// Bundles 1 and 2 finish before short ID metadata arrives.
		if unknown := ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
			MonitoringData: map[string][]byte{"c1": {8}},
		}); len(unknown) != 1 || unknown[0] != "c1" {
			t.Fatalf("bundle 1 unknown = %v, want [c1]", unknown)
		}
		if unknown := ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
			MonitoringData: map[string][]byte{"c1": {8}},
		}); len(unknown) != 1 || unknown[0] != "c1" {
			t.Fatalf("bundle 2 unknown = %v, want [c1]", unknown)
		}

		// Bundle 1 registers metadata first.
		ms.AddShortIDs(md)

		// Bundle 3 contributes against the resolved short ID.
		if unknown := ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
			MonitoringData: map[string][]byte{"c1": {8}},
		}); len(unknown) != 0 {
			t.Fatalf("bundle 3 unknown = %v, want empty", unknown)
		}

		// Bundle 2 delivers duplicate metadata after bundle 3 updated the accumulator.
		ms.AddShortIDs(md)

		want := []*pipepb.MonitoringInfo{
			makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_SUM_INT64, []byte{24}),
		}
		if diff := cmp.Diff(want, ms.Results(committed), protocmp.Transform()); diff != "" {
			t.Fatalf("Results(committed) diff (-want,+got):\n%s", diff)
		}
	})

	t.Run("secondShortIDSameMetricKey", func(t *testing.T) {
		ms := metricsStore{}

		ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
			MonitoringData: map[string][]byte{"c1": {8}},
		})
		ms.AddShortIDs(&fnpb.MonitoringInfosMetadataResponse{
			MonitoringInfo: map[string]*pipepb.MonitoringInfo{
				"c1": makeInfo(pipepb.MonitoringInfoSpecs_USER_SUM_INT64),
			},
		})

		ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
			MonitoringData: map[string][]byte{"c2": {5}},
		})
		ms.AddShortIDs(&fnpb.MonitoringInfosMetadataResponse{
			MonitoringInfo: map[string]*pipepb.MonitoringInfo{
				"c2": makeInfo(pipepb.MonitoringInfoSpecs_USER_SUM_INT64),
			},
		})

		want := []*pipepb.MonitoringInfo{
			makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_SUM_INT64, []byte{13}),
		}
		if diff := cmp.Diff(want, ms.Results(committed), protocmp.Transform()); diff != "" {
			t.Fatalf("Results(committed) diff (-want,+got):\n%s", diff)
		}
	})
}

func Test_metricsStore_ConcurrentShortIDResolution(t *testing.T) {
	ms := metricsStore{}
	md := &fnpb.MonitoringInfosMetadataResponse{
		MonitoringInfo: map[string]*pipepb.MonitoringInfo{
			"c1": makeInfo(pipepb.MonitoringInfoSpecs_USER_SUM_INT64),
		},
	}

	const bundles = 16
	var wg sync.WaitGroup
	wg.Add(bundles)
	for range bundles {
		go func() {
			defer wg.Done()
			ms.ContributeTentativeMetrics(&fnpb.ProcessBundleProgressResponse{
				MonitoringData: map[string][]byte{"c1": {3}},
			})
			unknown := ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
				MonitoringData: map[string][]byte{"c1": {2}},
			})
			if len(unknown) > 0 {
				ms.AddShortIDs(md)
			}
			_ = ms.Results(committed)
			_ = ms.Results(tentative)
		}()
	}
	wg.Wait()

	want := []*pipepb.MonitoringInfo{
		makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_SUM_INT64, []byte{2 * bundles}),
	}
	if diff := cmp.Diff(want, ms.Results(committed), protocmp.Transform()); diff != "" {
		t.Fatalf("Results(committed) diff (-want,+got):\n%s", diff)
	}
}

func Test_metricsStore_PartialAndUnknownShortIDs(t *testing.T) {
	t.Run("partialResolutionKeepsOtherBufferedShortIDs", func(t *testing.T) {
		ms := metricsStore{}
		ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
			MonitoringData: map[string][]byte{
				"c1": {4},
				"c2": {7},
			},
		})

		// Resolve only c1 first; c2 must stay buffered for a later metadata response.
		ms.AddShortIDs(&fnpb.MonitoringInfosMetadataResponse{
			MonitoringInfo: map[string]*pipepb.MonitoringInfo{
				"c1": makeInfo(pipepb.MonitoringInfoSpecs_USER_SUM_INT64),
			},
		})
		wantFirst := []*pipepb.MonitoringInfo{
			makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_SUM_INT64, []byte{4}),
		}
		if diff := cmp.Diff(wantFirst, ms.Results(committed), protocmp.Transform()); diff != "" {
			t.Fatalf("after first AddShortIDs diff (-want,+got):\n%s", diff)
		}

		ms.AddShortIDs(&fnpb.MonitoringInfosMetadataResponse{
			MonitoringInfo: map[string]*pipepb.MonitoringInfo{
				"c2": makeInfo(pipepb.MonitoringInfoSpecs_USER_SUM_INT64),
			},
		})
		wantBoth := []*pipepb.MonitoringInfo{
			makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_SUM_INT64, []byte{11}),
		}
		if diff := cmp.Diff(wantBoth, ms.Results(committed), protocmp.Transform()); diff != "" {
			t.Fatalf("after second AddShortIDs diff (-want,+got):\n%s", diff)
		}
	})

	t.Run("unknownUrnDropsBufferedPayloadsAndSkipsAccumulation", func(t *testing.T) {
		ms := metricsStore{}
		ms.ContributeTentativeMetrics(&fnpb.ProcessBundleProgressResponse{
			MonitoringData: map[string][]byte{"u1": {1}},
		})
		ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
			MonitoringData: map[string][]byte{"u1": {2}},
		})

		ms.AddShortIDs(&fnpb.MonitoringInfosMetadataResponse{
			MonitoringInfo: map[string]*pipepb.MonitoringInfo{
				"u1": {
					Urn:  "beam:metric:unknown_test_urn:v1",
					Type: "beam:metrics:sum_int64:v1",
				},
			},
		})
		if len(ms.unprocessedPayloads[tentative]) != 0 || len(ms.unprocessedPayloads[committed]) != 0 {
			t.Fatalf("expected unknown short ID payloads to be dropped, got %+v", ms.unprocessedPayloads)
		}

		// Also exercise accumulatePayload/contributeMetrics when shortIDsToKeys holds an unknown URN.
		ms.shortIDsToKeys = map[string]metricKey{
			"u2": ptransformKey{urn: "beam:metric:unknown_test_urn:v1", ptransform: "s1"},
		}
		if unknown := ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
			MonitoringData: map[string][]byte{"u2": {3}},
		}); len(unknown) != 0 {
			t.Fatalf("unexpected unknown IDs: %v", unknown)
		}
		if got := ms.Results(committed); len(got) != 0 {
			t.Fatalf("expected no committed results, got %v", got)
		}
	})

	t.Run("malformedPayloadPanics", func(t *testing.T) {
		ms := metricsStore{}
		ms.AddShortIDs(&fnpb.MonitoringInfosMetadataResponse{
			MonitoringInfo: map[string]*pipepb.MonitoringInfo{
				"bad": makeInfo(pipepb.MonitoringInfoSpecs_USER_SUM_INT64),
			},
		})
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic on truncated varint payload")
			}
		}()
		ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
			MonitoringData: map[string][]byte{"bad": {0x80}},
		})
	})
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		t.Fatalf("proto.Marshal(%v) failed: %v", m, err)
	}
	return b
}

func linearBuckets(start, width float64, numBuckets int32) *pipepb.HistogramValue_BucketOptions {
	return &pipepb.HistogramValue_BucketOptions{
		BucketType: &pipepb.HistogramValue_BucketOptions_Linear_{
			Linear: &pipepb.HistogramValue_BucketOptions_Linear{
				NumberOfBuckets: proto.Int32(numBuckets),
				Width:           proto.Float64(width),
				Start:           proto.Float64(start),
			},
		},
	}
}

func exponentialBuckets(scale, numBuckets int32) *pipepb.HistogramValue_BucketOptions {
	return &pipepb.HistogramValue_BucketOptions{
		BucketType: &pipepb.HistogramValue_BucketOptions_Exponential{
			Exponential: &pipepb.HistogramValue_BucketOptions_Base2Exponent{
				NumberOfBuckets: proto.Int32(numBuckets),
				Scale:           proto.Int32(scale),
			},
		},
	}
}

func Test_metricsStore_BoundedTrieTentativeVsCommitted(t *testing.T) {
	ms := metricsStore{}
	ms.AddShortIDs(&fnpb.MonitoringInfosMetadataResponse{
		MonitoringInfo: map[string]*pipepb.MonitoringInfo{
			"t": makeInfo(pipepb.MonitoringInfoSpecs_USER_BOUNDED_TRIE),
		},
	})

	first := mustMarshal(t, &pipepb.BoundedTrie{Bound: 100, Singleton: []string{"ns", "one"}})
	second := mustMarshal(t, &pipepb.BoundedTrie{Bound: 50, Singleton: []string{"ns", "two"}})

	ms.ContributeTentativeMetrics(&fnpb.ProcessBundleProgressResponse{
		MonitoringData: map[string][]byte{"t": first},
	})
	ms.ContributeTentativeMetrics(&fnpb.ProcessBundleProgressResponse{
		MonitoringData: map[string][]byte{"t": second},
	})

	gotTentative := ms.Results(tentative)
	if len(gotTentative) != 1 {
		t.Fatalf("Results(tentative) len = %d, want 1", len(gotTentative))
	}
	var tentativeTrie pipepb.BoundedTrie
	if err := proto.Unmarshal(gotTentative[0].GetPayload(), &tentativeTrie); err != nil {
		t.Fatalf("proto.Unmarshal tentative BoundedTrie failed: %v", err)
	}
	wantTentative := &pipepb.BoundedTrie{Bound: 50, Singleton: []string{"ns", "two"}}
	if diff := cmp.Diff(wantTentative, &tentativeTrie, protocmp.Transform()); diff != "" {
		t.Fatalf("tentative BoundedTrie diff (-want,+got):\n%v", diff)
	}

	ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
		MonitoringData: map[string][]byte{"t": first},
	})
	ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
		MonitoringData: map[string][]byte{"t": first},
	})
	ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
		MonitoringData: map[string][]byte{"t": second},
	})
	ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
		MonitoringData: map[string][]byte{"t": mustMarshal(t, &pipepb.BoundedTrie{Bound: 10})},
	})

	gotCommitted := ms.Results(committed)
	if len(gotCommitted) != 1 {
		t.Fatalf("Results(committed) len = %d, want 1", len(gotCommitted))
	}
	var committedTrie pipepb.BoundedTrie
	if err := proto.Unmarshal(gotCommitted[0].GetPayload(), &committedTrie); err != nil {
		t.Fatalf("proto.Unmarshal committed BoundedTrie failed: %v", err)
	}
	wantCommitted := &pipepb.BoundedTrie{
		Bound: 50,
		Root: &pipepb.BoundedTrieNode{
			Children: map[string]*pipepb.BoundedTrieNode{
				"ns": {
					Children: map[string]*pipepb.BoundedTrieNode{
						"one": {},
						"two": {},
					},
				},
			},
		},
	}
	if diff := cmp.Diff(wantCommitted, &committedTrie, protocmp.Transform()); diff != "" {
		t.Fatalf("committed BoundedTrie diff (-want,+got):\n%v", diff)
	}
}

func Test_metricsStore_HistogramTentativeVsCommitted(t *testing.T) {
	ms := metricsStore{}
	ms.AddShortIDs(&fnpb.MonitoringInfosMetadataResponse{
		MonitoringInfo: map[string]*pipepb.MonitoringInfo{
			"h": makeInfo(pipepb.MonitoringInfoSpecs_USER_HISTOGRAM),
		},
	})

	opts := linearBuckets(0, 10, 4)
	first := mustMarshal(t, &pipepb.HistogramValue{
		Count:         proto.Int64(2),
		BucketOptions: opts,
		BucketCounts:  []int64{1, 1, 0, 0},
	})
	second := mustMarshal(t, &pipepb.HistogramValue{
		Count:         proto.Int64(5),
		BucketOptions: opts,
		BucketCounts:  []int64{0, 2, 3, 0},
	})
	mismatched := mustMarshal(t, &pipepb.HistogramValue{
		Count:         proto.Int64(9),
		BucketOptions: linearBuckets(0, 20, 4),
		BucketCounts:  []int64{9, 0, 0, 0},
	})

	ms.ContributeTentativeMetrics(&fnpb.ProcessBundleProgressResponse{
		MonitoringData: map[string][]byte{"h": first},
	})
	ms.ContributeTentativeMetrics(&fnpb.ProcessBundleProgressResponse{
		MonitoringData: map[string][]byte{"h": second},
	})

	gotTentative := ms.Results(tentative)
	if len(gotTentative) != 1 {
		t.Fatalf("Results(tentative) len = %d, want 1", len(gotTentative))
	}
	var tentativeHist pipepb.HistogramValue
	if err := proto.Unmarshal(gotTentative[0].GetPayload(), &tentativeHist); err != nil {
		t.Fatalf("proto.Unmarshal tentative HistogramValue failed: %v", err)
	}
	wantTentative := &pipepb.HistogramValue{
		Count:         proto.Int64(5),
		BucketOptions: opts,
		BucketCounts:  []int64{0, 2, 3, 0},
	}
	if diff := cmp.Diff(wantTentative, &tentativeHist, protocmp.Transform()); diff != "" {
		t.Fatalf("tentative HistogramValue diff (-want,+got):\n%v", diff)
	}

	ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
		MonitoringData: map[string][]byte{"h": first},
	})
	ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
		MonitoringData: map[string][]byte{"h": second},
	})
	ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
		MonitoringData: map[string][]byte{"h": mismatched},
	})

	gotCommitted := ms.Results(committed)
	if len(gotCommitted) != 1 {
		t.Fatalf("Results(committed) len = %d, want 1", len(gotCommitted))
	}
	var committedHist pipepb.HistogramValue
	if err := proto.Unmarshal(gotCommitted[0].GetPayload(), &committedHist); err != nil {
		t.Fatalf("proto.Unmarshal committed HistogramValue failed: %v", err)
	}
	wantCommitted := &pipepb.HistogramValue{
		Count:         proto.Int64(7),
		BucketOptions: opts,
		BucketCounts:  []int64{1, 3, 3, 0},
	}
	if diff := cmp.Diff(wantCommitted, &committedHist, protocmp.Transform()); diff != "" {
		t.Fatalf("committed HistogramValue diff (-want,+got):\n%v", diff)
	}
}
