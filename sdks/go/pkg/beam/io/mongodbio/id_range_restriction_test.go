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
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/bson"
)

func Test_mergeFilters(t *testing.T) {
	tests := []struct {
		name     string
		idFilter bson.M
		filter   bson.M
		want     bson.M
	}{
		{
			name: "Merge ID filter and custom filter in an $and filter",
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
			name: "Keep only ID filter when custom filter is empty",
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
			name:     "Keep only custom filter when ID filter is empty",
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
			name:     "Empty filter when both ID filter and custom filter are empty",
			idFilter: bson.M{},
			filter:   bson.M{},
			want:     bson.M{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeFilters(tt.idFilter, tt.filter)
			if diff := cmp.Diff(got, tt.want); diff != "" {
				t.Errorf("mergeFilters() mismatch (-want +got): %v", diff)
			}
		})
	}
}

func Test_idRange_Filter(t *testing.T) {
	tests := []struct {
		name    string
		idRange idRange
		want    bson.M
	}{
		{
			name: "ID filter with $gte when min is inclusive",
			idRange: idRange{
				Min:          0,
				MinInclusive: true,
				Max:          10,
				MaxInclusive: false,
			},
			want: bson.M{
				"_id": bson.M{
					"$gte": 0,
					"$lt":  10,
				},
			},
		},
		{
			name: "ID filter with $gt when min is exclusive",
			idRange: idRange{
				Min:          0,
				MinInclusive: false,
				Max:          10,
				MaxInclusive: false,
			},
			want: bson.M{
				"_id": bson.M{
					"$gt": 0,
					"$lt": 10,
				},
			},
		},
		{
			name: "ID filter with $lte when max is inclusive",
			idRange: idRange{
				Min:          0,
				MinInclusive: true,
				Max:          10,
				MaxInclusive: true,
			},
			want: bson.M{
				"_id": bson.M{
					"$gte": 0,
					"$lte": 10,
				},
			},
		},
		{
			name: "ID filter with $lt when max is exclusive",
			idRange: idRange{
				Min:          0,
				MinInclusive: true,
				Max:          10,
				MaxInclusive: false,
			},
			want: bson.M{
				"_id": bson.M{
					"$gte": 0,
					"$lt":  10,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.idRange.Filter()
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Filter() mismatch (-want +got): %v", diff)
			}
		})
	}
}
