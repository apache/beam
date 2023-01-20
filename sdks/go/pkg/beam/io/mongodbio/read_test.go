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
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/bson"
)

func Test_calculateBucketCount(t *testing.T) {
	tests := []struct {
		name           string
		collectionSize int64
		bundleSize     int64
		want           int32
	}{
		{
			name:           "Return ceiling of collection size / bundle size",
			collectionSize: 3 * 1024 * 1024,
			bundleSize:     2 * 1024 * 1024,
			want:           2,
		},
		{
			name:           "Return max int32 when calculated count is greater than max int32",
			collectionSize: 1024 * 1024 * 1024 * 1024,
			bundleSize:     1,
			want:           math.MaxInt32,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := calculateBucketCount(tt.collectionSize, tt.bundleSize); got != tt.want {
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

func Test_idFiltersFromBuckets(t *testing.T) {
	tests := []struct {
		name    string
		buckets []bucket
		want    []bson.M
	}{
		{
			name: "Create one $lte filter for start range, one $gt filter for end range, and filters with both " +
				"$lte and $gt for ranges in between when there are three or more bucket elements",
			buckets: []bucket{
				{
					ID: minMax{
						Min: objectIDFromHex(t, "6384e03f24f854c1a8ce5378"),
						Max: objectIDFromHex(t, "6384e03f24f854c1a8ce5380"),
					},
				},
				{
					ID: minMax{
						Min: objectIDFromHex(t, "6384e03f24f854c1a8ce5380"),
						Max: objectIDFromHex(t, "6384e03f24f854c1a8ce5382"),
					},
				},
				{
					ID: minMax{
						Min: objectIDFromHex(t, "6384e03f24f854c1a8ce5382"),
						Max: objectIDFromHex(t, "6384e03f24f854c1a8ce5384"),
					},
				},
			},
			want: []bson.M{
				{
					"_id": bson.M{
						"$lte": objectIDFromHex(t, "6384e03f24f854c1a8ce5380"),
					},
				},
				{
					"_id": bson.M{
						"$gt":  objectIDFromHex(t, "6384e03f24f854c1a8ce5380"),
						"$lte": objectIDFromHex(t, "6384e03f24f854c1a8ce5382"),
					},
				},
				{
					"_id": bson.M{
						"$gt": objectIDFromHex(t, "6384e03f24f854c1a8ce5382"),
					},
				},
			},
		},
		{
			name: "Create one $lte filter for start range and one $gt filter for end range when there are two " +
				"bucket elements",
			buckets: []bucket{
				{
					ID: minMax{
						Min: objectIDFromHex(t, "6384e03f24f854c1a8ce5378"),
						Max: objectIDFromHex(t, "6384e03f24f854c1a8ce5380"),
					},
				},
				{
					ID: minMax{
						Min: objectIDFromHex(t, "6384e03f24f854c1a8ce5380"),
						Max: objectIDFromHex(t, "6384e03f24f854c1a8ce5382"),
					},
				},
			},
			want: []bson.M{
				{
					"_id": bson.M{
						"$lte": objectIDFromHex(t, "6384e03f24f854c1a8ce5380"),
					},
				},
				{
					"_id": bson.M{
						"$gt": objectIDFromHex(t, "6384e03f24f854c1a8ce5380"),
					},
				},
			},
		},
		{
			name: "Create an empty filter when there is one bucket element",
			buckets: []bucket{
				{
					ID: minMax{
						Min: objectIDFromHex(t, "6384e03f24f854c1a8ce5378"),
						Max: objectIDFromHex(t, "6384e03f24f854c1a8ce5380"),
					},
				},
			},
			want: []bson.M{{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := idFiltersFromBuckets(tt.buckets); !cmp.Equal(got, tt.want) {
				t.Errorf("idFiltersFromBuckets() = %v, want %v", got, tt.want)
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

func Test_idFiltersFromSplits(t *testing.T) {
	tests := []struct {
		name      string
		splitKeys []splitKey
		want      []bson.M
	}{
		{
			name: "Create one $lte filter for start range, one $gt filter for end range, and filters with both " +
				"$lte and $gt for ranges in between when there are two or more splitKey elements",
			splitKeys: []splitKey{
				{
					ID: objectIDFromHex(t, "6384e03f24f854c1a8ce5380"),
				},
				{
					ID: objectIDFromHex(t, "6384e03f24f854c1a8ce5382"),
				},
			},
			want: []bson.M{
				{
					"_id": bson.M{
						"$lte": objectIDFromHex(t, "6384e03f24f854c1a8ce5380"),
					},
				},
				{
					"_id": bson.M{
						"$gt":  objectIDFromHex(t, "6384e03f24f854c1a8ce5380"),
						"$lte": objectIDFromHex(t, "6384e03f24f854c1a8ce5382"),
					},
				},
				{
					"_id": bson.M{
						"$gt": objectIDFromHex(t, "6384e03f24f854c1a8ce5382"),
					},
				},
			},
		},
		{
			name: "Create one $lte filter for start range and one $gt filter for end range when there is one " +
				"splitKey element",
			splitKeys: []splitKey{
				{
					ID: objectIDFromHex(t, "6384e03f24f854c1a8ce5380"),
				},
			},
			want: []bson.M{
				{
					"_id": bson.M{
						"$lte": objectIDFromHex(t, "6384e03f24f854c1a8ce5380"),
					},
				},
				{
					"_id": bson.M{
						"$gt": objectIDFromHex(t, "6384e03f24f854c1a8ce5380"),
					},
				},
			},
		},
		{
			name:      "Create an empty filter when there are no splitKey elements",
			splitKeys: nil,
			want:      []bson.M{{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := idFiltersFromSplits(tt.splitKeys); !cmp.Equal(got, tt.want) {
				t.Errorf("idFiltersFromSplits() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_inferProjection(t *testing.T) {
	type doc struct {
		Field1 string `bson:"field1"`
		Field2 string `bson:"field2"`
		Field3 string `bson:"-"`
	}

	tests := []struct {
		name   string
		t      reflect.Type
		tagKey string
		want   bson.D
	}{
		{
			name:   "Infer projection from struct bson tags",
			t:      reflect.TypeOf(doc{}),
			tagKey: "bson",
			want: bson.D{
				{Key: "field1", Value: 1},
				{Key: "field2", Value: 1},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := inferProjection(tt.t, tt.tagKey); !cmp.Equal(got, tt.want) {
				t.Errorf("inferProjection() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_inferProjectionPanic(t *testing.T) {
	type doc struct{}

	t.Run("Panic when type has no fields to infer", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("inferProjection() does not panic")
			}
		}()

		inferProjection(reflect.TypeOf(doc{}), "bson")
	})
}

func Test_mergeFilters(t *testing.T) {
	tests := []struct {
		name     string
		idFilter bson.M
		filter   bson.M
		want     bson.M
	}{
		{
			name: "Returned merged ID filter and custom filter in an $and filter",
			idFilter: bson.M{
				"_id": bson.M{
					"$gte": 10,
				},
			},
			filter: bson.M{
				"key": bson.M{
					"$ne": "value",
				},
			},
			want: bson.M{
				"$and": []bson.M{
					{
						"_id": bson.M{
							"$gte": 10,
						},
					},
					{
						"key": bson.M{
							"$ne": "value",
						},
					},
				},
			},
		},
		{
			name: "Return only ID filter when custom filter is empty",
			idFilter: bson.M{
				"_id": bson.M{
					"$gte": 10,
				},
			},
			filter: bson.M{},
			want: bson.M{
				"_id": bson.M{
					"$gte": 10,
				},
			},
		},
		{
			name:     "Return only custom filter when ID filter is empty",
			idFilter: bson.M{},
			filter: bson.M{
				"key": bson.M{
					"$ne": "value",
				},
			},
			want: bson.M{
				"key": bson.M{
					"$ne": "value",
				},
			},
		},
		{
			name:     "Return empty filter when both ID filter and custom filter are empty",
			idFilter: bson.M{},
			filter:   bson.M{},
			want:     bson.M{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mergeFilters(tt.idFilter, tt.filter); !cmp.Equal(got, tt.want) {
				t.Errorf("mergeFilters() = %v, want %v", got, tt.want)
			}
		})
	}
}
