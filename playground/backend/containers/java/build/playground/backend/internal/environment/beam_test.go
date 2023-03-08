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

package environment

import (
	playground "beam.apache.org/playground/backend/internal/api/v1"
	"testing"
)

func TestBeamEnvs_PreparedModDir(t *testing.T) {
	preparedModDir := "testModDir"
	type fields struct {
		ApacheBeamSdk  playground.Sdk
		ExecutorConfig *ExecutorConfig
		preparedModDir string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "Get path to prepared directory of the go.mod",
			fields: fields{
				ApacheBeamSdk:  0,
				ExecutorConfig: nil,
				preparedModDir: preparedModDir,
			},
			want: preparedModDir,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BeamEnvs{
				ApacheBeamSdk:  tt.fields.ApacheBeamSdk,
				ExecutorConfig: tt.fields.ExecutorConfig,
				preparedModDir: tt.fields.preparedModDir,
			}
			if got := b.PreparedModDir(); got != tt.want {
				t.Errorf("PreparedModDir() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBeamEnvs_NumOfParallelJobs(t *testing.T) {
	numOfParallelJobs := 2
	type fields struct {
		ApacheBeamSdk     playground.Sdk
		ExecutorConfig    *ExecutorConfig
		preparedModDir    string
		numOfParallelJobs int
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{
			name: "Get the number of parallel jobs",
			fields: fields{
				ApacheBeamSdk:     0,
				ExecutorConfig:    nil,
				preparedModDir:    "",
				numOfParallelJobs: numOfParallelJobs,
			},
			want: numOfParallelJobs,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BeamEnvs{
				ApacheBeamSdk:     tt.fields.ApacheBeamSdk,
				ExecutorConfig:    tt.fields.ExecutorConfig,
				preparedModDir:    tt.fields.preparedModDir,
				numOfParallelJobs: tt.fields.numOfParallelJobs,
			}
			if got := b.NumOfParallelJobs(); got != tt.want {
				t.Errorf("NumOfParallelJobs() = %v, want %v", got, tt.want)
			}
		})
	}
}
