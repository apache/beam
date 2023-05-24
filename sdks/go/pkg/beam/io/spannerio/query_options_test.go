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

package spannerio

import (
	"cloud.google.com/go/spanner"
	"testing"
)

func TestWithBatching(t *testing.T) {
	tests := []struct {
		name     string
		batching bool
		want     bool
		wantErr  bool
	}{
		{
			name:     "Set batching to true",
			batching: true,
			want:     true,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var option queryOptions

			if err := WithBatching(tt.batching)(&option); (err != nil) != tt.wantErr {
				t.Fatalf("WithBatching() error = %v, wantErr %v", err, tt.wantErr)
			}

			if option.Batching != tt.want {
				t.Errorf("option.Batching = %v, want %v", option.Batching, tt.want)
			}
		})
	}
}

func TestWithMaxPartitions(t *testing.T) {
	tests := []struct {
		name          string
		maxPartitions int64
		want          int64
		wantErr       bool
	}{
		{
			name:          "Set max partitions to 1024",
			maxPartitions: 1024,
			want:          1024,
			wantErr:       false,
		},
		{
			name:          "Error - max partitions must be greater than 0",
			maxPartitions: 0,
			wantErr:       true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var option queryOptions

			if err := WithMaxPartitions(tt.maxPartitions)(&option); (err != nil) != tt.wantErr {
				t.Fatalf("WithMaxPartitions() error = %v, wantErr %v", err, tt.wantErr)
			}

			if option.MaxPartitions != tt.want {
				t.Errorf("option.MaxPartitions = %v, want %v", option.MaxPartitions, tt.want)
			}
		})
	}
}
func TestWithTimestampbound(t *testing.T) {
	tests := []struct {
		name           string
		timestampBound spanner.TimestampBound
		want           spanner.TimestampBound
		wantErr        bool
	}{
		{
			name:           "Set timestamp bound to strong",
			timestampBound: spanner.StrongRead(),
			want:           spanner.StrongRead(),
			wantErr:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var option queryOptions

			if err := WithTimestampBound(tt.timestampBound)(&option); (err != nil) != tt.wantErr {
				t.Fatalf("WithTimestampBound() error = %v, wantErr %v", err, tt.wantErr)
			}

			if option.TimestampBound != tt.want {
				t.Errorf("option.TimestampBound = %v, want %v", option.TimestampBound, tt.want)
			}
		})
	}
}
