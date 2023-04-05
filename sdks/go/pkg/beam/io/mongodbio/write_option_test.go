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
)

func TestWithWriteBatchSize(t *testing.T) {
	tests := []struct {
		name      string
		batchSize int64
		want      int64
		wantErr   bool
	}{
		{
			name:      "Set batch size to 500",
			batchSize: 500,
			want:      500,
			wantErr:   false,
		},
		{
			name:      "Error - batch size must be greater than 0",
			batchSize: 0,
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var option WriteOption

			if err := WithWriteBatchSize(tt.batchSize)(&option); (err != nil) != tt.wantErr {
				t.Fatalf("WithWriteBatchSize() error = %v, wantErr %v", err, tt.wantErr)
			}

			if option.BatchSize != tt.want {
				t.Errorf("option.BatchSize = %v, want %v", option.BatchSize, tt.want)
			}
		})
	}
}

func TestWithWriteOrdered(t *testing.T) {
	tests := []struct {
		name    string
		ordered bool
		want    bool
		wantErr bool
	}{
		{
			name:    "Set ordered to true",
			ordered: true,
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var option WriteOption

			if err := WithWriteOrdered(tt.ordered)(&option); (err != nil) != tt.wantErr {
				t.Fatalf("WithWriteOrdered() err = %v, wantErr %v", err, tt.wantErr)
			}

			if option.Ordered != tt.want {
				t.Errorf("option.Ordered = %v, want %v", option.Ordered, tt.want)
			}
		})
	}
}
