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

package streaming

import (
	"beam.apache.org/playground/backend/internal/cache"
	"beam.apache.org/playground/backend/internal/cache/local"
	"context"
	"github.com/google/uuid"
	"testing"
)

func TestRunOutputWriter_Write(t *testing.T) {
	pipelineId := uuid.New()
	cacheService := local.New(context.Background())
	err := cacheService.SetValue(context.Background(), pipelineId, cache.RunOutput, "MOCK_TEST_OUTPUT")
	if err != nil {
		panic(err)
	}

	type fields struct {
		Ctx          context.Context
		CacheService cache.Cache
		PipelineId   uuid.UUID
	}
	type args struct {
		p []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		check   func() bool
		want    int
		wantErr bool
	}{
		{
			// Test case with calling Write method with empty string parameter.
			// As a result, want to receive 0.
			name: "empty string",
			fields: fields{
				Ctx:          context.Background(),
				CacheService: cacheService,
				PipelineId:   uuid.New(),
			},
			args: args{[]byte("")},
			check: func() bool {
				return true
			},
			want:    0,
			wantErr: false,
		},
		{
			// Test case with calling Write method with pipelineId which doesn't contain output yet.
			// As a result, want to receive an error.
			name: "output doesn't exist",
			fields: fields{
				Ctx:          context.Background(),
				CacheService: cacheService,
				PipelineId:   uuid.New(),
			},
			args: args{[]byte(" NEW_MOCK_OUTPUT")},
			check: func() bool {
				return true
			},
			want:    0,
			wantErr: true,
		},
		{
			// Test case with calling Write method with pipelineId which already contains output.
			// As a result, want to receive len of sent string.
			name: "output exists",
			fields: fields{
				Ctx:          context.Background(),
				CacheService: cacheService,
				PipelineId:   pipelineId,
			},
			args: args{[]byte(" NEW_MOCK_OUTPUT")},
			check: func() bool {
				value, err := cacheService.GetValue(context.Background(), pipelineId, cache.RunOutput)
				if err != nil {
					return false
				}
				return value == "MOCK_TEST_OUTPUT NEW_MOCK_OUTPUT"
			},
			want:    16,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := &RunOutputWriter{
				Ctx:          tt.fields.Ctx,
				CacheService: tt.fields.CacheService,
				PipelineId:   tt.fields.PipelineId,
			}
			got, err := row.Write(tt.args.p)
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Write() got = %v, want %v", got, tt.want)
			}
			if !tt.check() {
				t.Errorf("Write() writes unexpectable data to cache")
			}
		})
	}
}
