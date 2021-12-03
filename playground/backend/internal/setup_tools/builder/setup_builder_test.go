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

package builder

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/executors"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"beam.apache.org/playground/backend/internal/utils"
	"fmt"
	"github.com/google/uuid"
	"testing"
)

func TestSetupExecutor(t *testing.T) {
	pipelineId := uuid.New()
	sdk := pb.Sdk_SDK_JAVA
	lc, err := fs_tool.NewLifeCycle(sdk, pipelineId, "")
	if err != nil {
		panic(err)
	}
	executorConfig := &environment.ExecutorConfig{
		CompileCmd:  "MOCK_COMPILE_CMD",
		RunCmd:      "MOCK_RUN_CMD",
		CompileArgs: []string{"MOCK_COMPILE_ARG"},
		RunArgs:     []string{"MOCK_RUN_ARG"},
	}

	sdkEnv := environment.NewBeamEnvs(sdk, executorConfig, "")
	val, err := utils.GetValidators(sdk, lc.GetAbsoluteSourceFilePath())
	if err != nil {
		panic(err)
	}
	prep, err := utils.GetPreparators(sdk, lc.GetAbsoluteSourceFilePath())
	if err != nil {
		panic(err)
	}

	wantExecutor := executors.NewExecutorBuilder().
		WithValidator().
		WithSdkValidators(val).
		WithPreparator().
		WithSdkPreparators(prep).
		WithCompiler().
		WithCommand(executorConfig.CompileCmd).
		WithArgs(executorConfig.CompileArgs).
		WithFileName(lc.GetAbsoluteSourceFilePath()).
		WithWorkingDir(lc.GetAbsoluteBaseFolderPath()).
		WithRunner().
		WithCommand(sdkEnv.ExecutorConfig.RunCmd).
		WithArgs(sdkEnv.ExecutorConfig.RunArgs).
		WithWorkingDir(lc.GetAbsoluteBaseFolderPath())

	type args struct {
		srcFilePath    string
		baseFolderPath string
		execFilePath   string
		sdkEnv         *environment.BeamEnvs
	}
	tests := []struct {
		name    string
		args    args
		want    *executors.RunBuilder
		wantErr bool
	}{
		{
			// Test case with calling Setup with incorrect SDK.
			// As a result, want to receive an error.
			name:    "incorrect sdk",
			args:    args{lc.GetAbsoluteSourceFilePath(), lc.GetAbsoluteBaseFolderPath(), lc.GetAbsoluteExecutableFilePath(), environment.NewBeamEnvs(pb.Sdk_SDK_UNSPECIFIED, executorConfig, "")},
			want:    nil,
			wantErr: true,
		},
		{
			// Test case with calling Setup with correct SDK.
			// As a result, want to receive an expected builder.
			name:    "correct sdk",
			args:    args{lc.GetAbsoluteSourceFilePath(), lc.GetAbsoluteBaseFolderPath(), lc.GetAbsoluteExecutableFilePath(), sdkEnv},
			want:    wantExecutor,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SetupExecutorBuilder(tt.args.srcFilePath, tt.args.baseFolderPath, tt.args.execFilePath, tt.args.sdkEnv)
			if (err != nil) != tt.wantErr {
				t.Errorf("SetupExecutorBuilder() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && fmt.Sprint(got.Build()) != fmt.Sprint(tt.want.Build()) {
				t.Errorf("SetupExecutorBuilder() got = %v, want %v", got.Build(), tt.want.Build())
			}
		})
	}
}
