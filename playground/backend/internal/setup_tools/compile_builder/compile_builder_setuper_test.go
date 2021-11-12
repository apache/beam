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

package compile_builder

import (
	playground "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/executors"
	"beam.apache.org/playground/backend/internal/validators"
	"context"
	"github.com/google/uuid"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSetup(t *testing.T) {
	pipelineId := uuid.New()
	sdk := playground.Sdk_SDK_JAVA
	filesFolderPath := filepath.Join("executable_files", pipelineId.String())
	filePath := filepath.Join(filesFolderPath, "src", pipelineId.String()+".java")
	executorConfig := &environment.ExecutorConfig{
		CompileCmd:  "MOCK_COMPILE_CMD",
		RunCmd:      "MOCK_RUN_CMD",
		CompileArgs: []string{"MOCK_COMPILE_ARG"},
		RunArgs:     []string{"MOCK_RUN_ARG"},
	}
	val := validators.GetJavaValidators(filePath)

	type args struct {
		filePath        string
		filesFolderPath string
		sdk             playground.Sdk
		executorConfig  *environment.ExecutorConfig
	}
	tests := []struct {
		name    string
		args    args
		want    *executors.CompileBuilder
		wantErr bool
	}{
		{
			// Test case with calling Setup with incorrect SDK.
			// As a result, want to receive an error.
			name: "incorrect sdk",
			args: args{
				filePath:        filePath,
				filesFolderPath: filesFolderPath,
				sdk:             playground.Sdk_SDK_UNSPECIFIED,
				executorConfig:  executorConfig,
			},
			want:    nil,
			wantErr: true,
		},
		{
			// Test case with calling Setup with correct SDK.
			// As a result, want to receive an expected compile builder.
			name: "correct sdk",
			args: args{
				filePath:        filePath,
				filesFolderPath: filesFolderPath,
				sdk:             sdk,
				executorConfig:  executorConfig,
			},
			want: executors.NewExecutorBuilder().
				WithValidator().
				WithSdkValidators(val).
				WithCompiler().
				WithCommand(executorConfig.CompileCmd).
				WithArgs(executorConfig.CompileArgs).
				WithFileName(filePath).
				WithWorkingDir(filesFolderPath),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Setup(tt.args.filePath, tt.args.filesFolderPath, tt.args.sdk, tt.args.executorConfig)
			if (err != nil) != tt.wantErr {
				t.Errorf("Setup() err = %v, wantErr %v", err, tt.wantErr)
			}
			if got != nil {
				gotBuilder := got.Build()
				wantBuilder := tt.want.Build()
				if !reflect.DeepEqual(gotBuilder.Compile(context.Background()), wantBuilder.Compile(context.Background())) {
					t.Errorf("Setup() got.Build.Compile() = %v, want %v", gotBuilder.Compile(context.Background()), wantBuilder.Compile(context.Background()))
				}
				if !reflect.DeepEqual(gotBuilder.Run(context.Background()), wantBuilder.Run(context.Background())) {
					t.Errorf("Setup() got.Build.Run() = %v, want %v", gotBuilder.Run(context.Background()), wantBuilder.Run(context.Background()))
				}
			}
		})
	}
}
