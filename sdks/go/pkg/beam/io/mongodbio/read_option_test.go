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

func TestWithReadBucketAuto(t *testing.T) {
	tests := []struct {
		name       string
		bucketAuto bool
		want       bool
		wantErr    bool
	}{
		{
			name:       "Set bucket auto to true",
			bucketAuto: true,
			want:       true,
			wantErr:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var option ReadOption

			if err := WithReadBucketAuto(tt.bucketAuto)(&option); (err != nil) != tt.wantErr {
				t.Fatalf("WithReadBucketAuto() error = %v, wantErr %v", err, tt.wantErr)
			}

			if option.BucketAuto != tt.want {
				t.Errorf("option.BucketAuto = %v, want %v", option.BucketAuto, tt.want)
			}
		})
	}
}

func TestWithReadFilter(t *testing.T) {
	tests := []struct {
		name    string
		filter  bson.M
		want    bson.M
		wantErr bool
	}{
		{
			name:    "Set filter to {\"key\": \"value\"}",
			filter:  bson.M{"key": "value"},
			want:    bson.M{"key": "value"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var option ReadOption

			if err := WithReadFilter(tt.filter)(&option); (err != nil) != tt.wantErr {
				t.Fatalf("WithReadFilter() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !cmp.Equal(option.Filter, tt.want) {
				t.Errorf("option.Filter = %v, want %v", option.Filter, tt.want)
			}
		})
	}
}

func TestWithReadBundleSize(t *testing.T) {
	tests := []struct {
		name       string
		bundleSize int64
		want       int64
		wantErr    bool
	}{
		{
			name:       "Set bundle size to 1024",
			bundleSize: 1024,
			want:       1024,
			wantErr:    false,
		},
		{
			name:       "Error - bundle size must be greater than 0",
			bundleSize: 0,
			wantErr:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var option ReadOption

			if err := WithReadBundleSize(tt.bundleSize)(&option); (err != nil) != tt.wantErr {
				t.Fatalf("WithReadBundleSize() error = %v, wantErr %v", err, tt.wantErr)
			}

			if option.BundleSize != tt.want {
				t.Errorf("option.BundleSize = %v, want %v", option.BundleSize, tt.want)
			}
		})
	}
}
