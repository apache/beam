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

package run_builder

import (
	playground "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/executors"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"context"
	"github.com/google/uuid"
	"os"
	"reflect"
	"testing"
)

func TestSetup(t *testing.T) {
	successPipelineId := uuid.New()
	errorPipelineId := uuid.New()
	sdk := playground.Sdk_SDK_JAVA
	errorLc, err := fs_tool.NewLifeCycle(sdk, errorPipelineId, "")
	if err != nil {
		panic(err)
	}
	successLc, err := fs_tool.NewLifeCycle(sdk, successPipelineId, "")
	if err != nil {
		panic(err)
	}
	err = successLc.CreateFolders()
	if err != nil {
		panic(err)
	}
	_, err = os.Create(successLc.Folder.ExecutableFileFolder + "/temp.class")
	if err != nil {
		panic(err)
	}
	executorConfig := &environment.ExecutorConfig{
		CompileCmd:  "MOCK_COMPILE_CMD",
		RunCmd:      "MOCK_RUN_CMD",
		CompileArgs: []string{"MOCK_COMPILE_ARG"},
		RunArgs:     []string{"MOCK_RUN_ARG"},
	}
	sdkEnv := environment.NewBeamEnvs(playground.Sdk_SDK_JAVA, executorConfig)
	compileBuilder := &executors.CompileBuilder{}
	className, err := successLc.ExecutableName(successPipelineId, "")
	if err != nil {
		panic(err)
	}

	type args struct {
		pipelineId     uuid.UUID
		lc             *fs_tool.LifeCycle
		workingDir     string
		sdkEnv         *environment.BeamEnvs
		compileBuilder *executors.CompileBuilder
	}
	tests := []struct {
		name    string
		args    args
		want    *executors.RunBuilder
		wantErr bool
	}{
		{
			// Test case with calling Setup method with incorrect SDK.
			// As a result, want to receive an error.
			name: "incorrect sdk",
			args: args{
				pipelineId:     successPipelineId,
				lc:             successLc,
				workingDir:     "",
				sdkEnv:         environment.NewBeamEnvs(playground.Sdk_SDK_UNSPECIFIED, executorConfig),
				compileBuilder: compileBuilder,
			},
			want:    nil,
			wantErr: true,
		},
		{
			// Test case with calling Setup method with incorrect LifeCycle.
			// As a result, want to receive an error.
			name: "error during set class name",
			args: args{
				pipelineId:     errorPipelineId,
				lc:             errorLc,
				workingDir:     "",
				sdkEnv:         sdkEnv,
				compileBuilder: compileBuilder,
			},
			want:    nil,
			wantErr: true,
		},
		{
			// Test case with calling Setup method with correct SDK.
			// As a result, want to receive an expected run builder.
			name: "correct sdk",
			args: args{
				pipelineId:     successPipelineId,
				lc:             successLc,
				workingDir:     "",
				sdkEnv:         sdkEnv,
				compileBuilder: compileBuilder,
			},
			want: compileBuilder.
				WithRunner().
				WithCommand(sdkEnv.ExecutorConfig.RunCmd).
				WithArgs(sdkEnv.ExecutorConfig.RunArgs).
				WithWorkingDir(successLc.GetAbsoluteBaseFolderPath()).
				WithExecutableName(className),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Setup(tt.args.pipelineId, tt.args.lc, tt.args.workingDir, tt.args.sdkEnv, tt.args.compileBuilder)
			if (err != nil) != tt.wantErr {
				t.Errorf("Setup() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil {
				gotBuilder := got.Build()
				wantBuilder := tt.want.Build()
				if !reflect.DeepEqual(gotBuilder.Run(context.Background()), wantBuilder.Run(context.Background())) {
					t.Errorf("Setup() got.Build().Run() = %v, want %v", gotBuilder.Run(context.Background()), wantBuilder.Run(context.Background()))
				}
				if !reflect.DeepEqual(gotBuilder.Compile(context.Background()), wantBuilder.Compile(context.Background())) {
					t.Errorf("Setup() got.Build().Compile() = %v, want %v", gotBuilder.Compile(context.Background()), wantBuilder.Compile(context.Background()))
				}
				os.RemoveAll("executable_files")
			}
		})
	}
}
