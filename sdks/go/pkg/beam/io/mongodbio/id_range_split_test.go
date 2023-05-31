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

package mongodbio

import (
	"math"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func Test_calculateBucketCount(t *testing.T) {
	tests := []struct {
		name       string
		totalSize  int64
		bundleSize int64
		want       int32
	}{
		{
			name:       "Return ceiling of total size / bundle size",
			totalSize:  3 * 1024 * 1024,
			bundleSize: 2 * 1024 * 1024,
			want:       2,
		},
		{
			name:       "Return max int32 when calculated count is greater than max int32",
			totalSize:  1024 * 1024 * 1024 * 1024,
			bundleSize: 1,
			want:       math.MaxInt32,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := calculateBucketCount(tt.totalSize, tt.bundleSize); got != tt.want {
				t.Errorf("calculateBucketCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_calculateBucketCountPanic(t *testing.T) {
	t.Run("Panic when bundleSize is not greater than 0", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("calculateBucketCount() does not panic")
			}
		}()

		calculateBucketCount(1024, 0)
	})
}

func Test_idRangesFromBuckets(t *testing.T) {
	tests := []struct {
		name       string
		buckets    []bucket
		outerRange idRange
		want       []idRange
	}{
		{
			name: "ID ranges with first element having min ID configuration from outer range, and last element " +
				"having max ID configuration from outer range",
			buckets: []bucket{
				{
					ID: minMax{
						Min: 5,
						Max: 100,
					},
				},
				{
					ID: minMax{
						Min: 100,
						Max: 200,
					},
				},
				{
					ID: minMax{
						Min: 200,
						Max: 295,
					},
				},
			},
			outerRange: idRange{
				Min:          0,
				MinInclusive: false,
				Max:          300,
				MaxInclusive: false,
			},
			want: []idRange{
				{
					Min:          0,
					MinInclusive: false,
					Max:          100,
					MaxInclusive: false,
				},
				{
					Min:          100,
					MinInclusive: true,
					Max:          200,
					MaxInclusive: false,
				},
				{
					Min:          200,
					MinInclusive: true,
					Max:          300,
					MaxInclusive: false,
				},
			},
		},
		{
			name: "ID ranges with one element having the same configuration as outer range when there is one " +
				"element in buckets",
			buckets: []bucket{
				{
					ID: minMax{
						Min: 5,
						Max: 95,
					},
				},
			},
			outerRange: idRange{
				Min:          0,
				MinInclusive: false,
				Max:          100,
				MaxInclusive: false,
			},
			want: []idRange{
				{
					Min:          0,
					MinInclusive: false,
					Max:          100,
					MaxInclusive: false,
				},
			},
		},
		{
			name:    "Empty ID ranges when there are no elements in buckets",
			buckets: nil,
			outerRange: idRange{
				Min:          0,
				MinInclusive: false,
				Max:          100,
				MaxInclusive: false,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := idRangesFromBuckets(tt.buckets, tt.outerRange)
			if diff := cmp.Diff(got, tt.want); diff != "" {
				t.Errorf("idRangesFromBuckets() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func Test_getChunkSize(t *testing.T) {
	tests := []struct {
		name       string
		bundleSize int64
		want       int64
	}{
		{
			name:       "Return 1 MB if bundle size is less than 1 MB",
			bundleSize: 1024,
			want:       1024 * 1024,
		},
		{
			name:       "Return 1 GB if bundle size is greater than 1 GB",
			bundleSize: 2 * 1024 * 1024 * 1024,
			want:       1024 * 1024 * 1024,
		},
		{
			name:       "Return bundle size if bundle size is between 1 MB and 1 GB",
			bundleSize: 4 * 1024 * 1024,
			want:       4 * 1024 * 1024,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getChunkSize(tt.bundleSize); got != tt.want {
				t.Errorf("getChunkSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_idRangesFromSplits(t *testing.T) {
	tests := []struct {
		name       string
		splitKeys  []documentID
		outerRange idRange
		want       []idRange
	}{
		{
			name: "ID ranges with first element having min ID configuration from outer range, and last element " +
				"having max ID configuration from outer range",
			splitKeys: []documentID{
				{
					ID: 100,
				},
				{
					ID: 200,
				},
			},
			outerRange: idRange{
				Min:          0,
				MinInclusive: false,
				Max:          300,
				MaxInclusive: false,
			},
			want: []idRange{
				{
					Min:          0,
					MinInclusive: false,
					Max:          100,
					MaxInclusive: false,
				},
				{
					Min:          100,
					MinInclusive: true,
					Max:          200,
					MaxInclusive: false,
				},
				{
					Min:          200,
					MinInclusive: true,
					Max:          300,
					MaxInclusive: false,
				},
			},
		},
		{
			name: "ID ranges with one element having the same configuration as outer range when there are no " +
				"elements in key splits",
			splitKeys: nil,
			outerRange: idRange{
				Min:          0,
				MinInclusive: true,
				Max:          100,
				MaxInclusive: true,
			},
			want: []idRange{
				{
					Min:          0,
					MinInclusive: true,
					Max:          100,
					MaxInclusive: true,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := idRangesFromSplits(tt.splitKeys, tt.outerRange)
			if diff := cmp.Diff(got, tt.want); diff != "" {
				t.Errorf("idRangesFromSplits() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
