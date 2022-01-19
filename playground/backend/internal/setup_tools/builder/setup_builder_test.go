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
	"beam.apache.org/playground/backend/internal/validators"
	"fmt"
	"github.com/google/uuid"
	"reflect"
	"strings"
	"sync"
	"testing"
)

var paths *fs_tool.LifeCyclePaths
var sdkEnv *environment.BeamEnvs

func TestMain(m *testing.M) {
	setup()
	m.Run()
}

func setup() {
	pipelineId := uuid.New()
	sdk := pb.Sdk_SDK_PYTHON
	lc, _ := fs_tool.NewLifeCycle(sdk, pipelineId, "")
	paths = &lc.Paths
	executorConfig := &environment.ExecutorConfig{
		CompileCmd:  "MOCK_COMPILE_CMD",
		CompileArgs: []string{"MOCK_COMPILE_ARG"},
	}
	sdkEnv = environment.NewBeamEnvs(sdk, executorConfig, "", "", 0)
}

func TestValidator(t *testing.T) {
	vals, err := utils.GetValidators(sdkEnv.ApacheBeamSdk, paths.AbsoluteSourceFilePath)
	if err != nil {
		panic(err)
	}
	wantExecutor := executors.NewExecutorBuilder().
		WithValidator().
		WithSdkValidators(vals)

	wrongSdkEnv := environment.NewBeamEnvs(pb.Sdk_SDK_UNSPECIFIED, sdkEnv.ExecutorConfig, "", "", 0)

	type args struct {
		paths  *fs_tool.LifeCyclePaths
		sdkEnv *environment.BeamEnvs
	}
	tests := []struct {
		name    string
		args    args
		want    *executors.ExecutorBuilder
		wantErr bool
	}{
		{
			// Test case with calling Setup with correct data.
			// As a result, want to receive an expected validator builder.
			name: "Test correct validator builder",
			args: args{
				paths:  paths,
				sdkEnv: sdkEnv,
			},
			want:    &wantExecutor.ExecutorBuilder,
			wantErr: false,
		},
		{
			// Test case with calling Setup with incorrect SDK.
			// As a result, want to receive an error.
			name: "incorrect sdk",
			args: args{
				paths:  paths,
				sdkEnv: wrongSdkEnv,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Validator(tt.args.paths, tt.args.sdkEnv)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validator() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Validator() got = %v, want %v", got, tt.want)
				return
			}
			if err == nil && !reflect.DeepEqual(fmt.Sprint(got.Build()), fmt.Sprint(tt.want.Build())) {
				t.Errorf("Validator() got = %v\n, want %v", got.Build(), tt.want.Build())
			}
		})
	}
}

func TestPreparer(t *testing.T) {
	validationResults := sync.Map{}
	validationResults.Store(validators.UnitTestValidatorName, false)
	validationResults.Store(validators.KatasValidatorName, false)

	prep, err := utils.GetPreparers(sdkEnv.ApacheBeamSdk, paths.AbsoluteSourceFilePath, &validationResults)
	if err != nil {
		panic(err)
	}
	pipelineOptions := ""
	wantExecutor := executors.NewExecutorBuilder().
		WithPreparer().
		WithSdkPreparers(prep)

	wrongSdkEnv := environment.NewBeamEnvs(pb.Sdk_SDK_UNSPECIFIED, sdkEnv.ExecutorConfig, "", "", 0)

	type args struct {
		paths           fs_tool.LifeCyclePaths
		pipelineOptions string
		sdkEnv          *environment.BeamEnvs
		valResults      *sync.Map
	}
	tests := []struct {
		name    string
		args    args
		want    *executors.ExecutorBuilder
		wantErr bool
	}{
		{
			// Test case with calling Setup with incorrect SDK.
			// As a result, want to receive an error.
			name:    "incorrect sdk",
			args:    args{*paths, pipelineOptions, wrongSdkEnv, &validationResults},
			want:    nil,
			wantErr: true,
		},
		{
			// Test case with calling Setup with correct SDK.
			// As a result, want to receive an expected preparer builder.
			name:    "correct sdk",
			args:    args{*paths, pipelineOptions, sdkEnv, &validationResults},
			want:    &wantExecutor.ExecutorBuilder,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Preparer(&tt.args.paths, tt.args.sdkEnv, tt.args.valResults)
			if (err != nil) != tt.wantErr {
				t.Errorf("Preparer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Preparer() got = %v, want %v", got, tt.want)
				return
			}
			if err == nil && !reflect.DeepEqual(fmt.Sprint(got.Build()), fmt.Sprint(tt.want.Build())) {
				t.Errorf("Preparer() got = %v, want %v", got.Build(), tt.want.Build())
			}
		})
	}
}

func TestCompiler(t *testing.T) {
	wantExecutor := executors.NewExecutorBuilder().
		WithCompiler().
		WithCommand(sdkEnv.ExecutorConfig.CompileCmd).
		WithWorkingDir(paths.AbsoluteBaseFolderPath).
		WithArgs(sdkEnv.ExecutorConfig.CompileArgs).
		WithFileName(paths.AbsoluteSourceFilePath)

	type args struct {
		paths  *fs_tool.LifeCyclePaths
		sdkEnv *environment.BeamEnvs
	}
	tests := []struct {
		name string
		args args
		want *executors.ExecutorBuilder
	}{
		{
			// Test case with calling Setup with correct data.
			// As a result, want to receive an expected compiler builder.
			name: "Test correct compiler builder",
			args: args{
				paths:  paths,
				sdkEnv: sdkEnv,
			},
			want: &wantExecutor.ExecutorBuilder,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Compiler(tt.args.paths, tt.args.sdkEnv)
			if !reflect.DeepEqual(fmt.Sprint(got.Build()), fmt.Sprint(tt.want.Build())) {
				t.Errorf("Compiler() = %v, want %v", got.Build(), tt.want.Build())
			}
		})
	}
}

func TestRunnerBuilder(t *testing.T) {
	wantExecutor := executors.NewExecutorBuilder().
		WithRunner().
		WithExecutableFileName(paths.AbsoluteExecutableFilePath).
		WithWorkingDir(paths.AbsoluteBaseFolderPath).
		WithCommand(sdkEnv.ExecutorConfig.RunCmd).
		WithArgs(sdkEnv.ExecutorConfig.RunArgs).
		WithPipelineOptions(strings.Split("", " "))

	type args struct {
		paths           *fs_tool.LifeCyclePaths
		pipelineOptions string
		sdkEnv          *environment.BeamEnvs
	}
	tests := []struct {
		name string
		args args
		want *executors.ExecutorBuilder
	}{
		{
			// Test case with calling Setup with correct data.
			// As a result, want to receive an expected run builder.
			name: "Test correct run builder",
			args: args{
				paths:  paths,
				sdkEnv: sdkEnv,
			},
			want: &wantExecutor.ExecutorBuilder,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := Runner(tt.args.paths, tt.args.pipelineOptions, tt.args.sdkEnv)
			if !reflect.DeepEqual(fmt.Sprint(got.Build()), fmt.Sprint(tt.want.Build())) {
				t.Errorf("Runner() got = %v, want %v", got.Build(), tt.want.Build())
			}
		})
	}
}

func TestTestRunner(t *testing.T) {
	wantExecutor := executors.NewExecutorBuilder().
		WithTestRunner().
		WithExecutableFileName(paths.AbsoluteExecutableFilePath).
		WithCommand(sdkEnv.ExecutorConfig.TestCmd).
		WithArgs(sdkEnv.ExecutorConfig.TestArgs).
		WithWorkingDir(paths.AbsoluteSourceFileFolderPath)

	type args struct {
		paths  *fs_tool.LifeCyclePaths
		sdkEnv *environment.BeamEnvs
	}
	tests := []struct {
		name string
		args args
		want *executors.ExecutorBuilder
	}{
		{
			// Test case with calling Setup with correct data.
			// As a result, want to receive an expected test builder.
			name: "Test correct test builder",
			args: args{
				paths:  paths,
				sdkEnv: sdkEnv,
			},
			want: &wantExecutor.ExecutorBuilder,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := TestRunner(tt.args.paths, tt.args.sdkEnv)
			if !reflect.DeepEqual(fmt.Sprint(got.Build()), fmt.Sprint(tt.want.Build())) {
				t.Errorf("TestRunner() got = %v, want %v", got.Build(), tt.want.Build())
			}
		})
	}
}

func Test_replaceLogPlaceholder(t *testing.T) {
	type args struct {
		paths          *fs_tool.LifeCyclePaths
		executorConfig *environment.ExecutorConfig
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "Test to check the replacement of log work with no error",
			args: args{
				paths:          paths,
				executorConfig: sdkEnv.ExecutorConfig,
			},
			want: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := replaceLogPlaceholder(tt.args.paths, tt.args.executorConfig); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("replaceLogPlaceholder() = %v, want %v", got, tt.want)
			}
		})
	}
}
