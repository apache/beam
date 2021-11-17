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

package code_processing

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/cache"
	"beam.apache.org/playground/backend/internal/cache/local"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/executors"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"beam.apache.org/playground/backend/internal/validators"
	"context"
	"fmt"
	"github.com/google/uuid"
	"go.uber.org/goleak"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const javaConfig = "{\n  \"compile_cmd\": \"javac\",\n  \"run_cmd\": \"java\",\n  \"compile_args\": [\"-d\", \"bin\", \"-classpath\"],\n  \"run_args\": [\"-cp\", \"bin:\"]\n}"

var opt goleak.Option
var cacheService cache.Cache

func TestMain(m *testing.M) {
	setup()
	opt = goleak.IgnoreCurrent()
	exitValue := m.Run()
	teardown()
	os.Exit(exitValue)
}

func setup() {
	// create configs for java
	err := os.MkdirAll("configs", fs.ModePerm)
	if err != nil {
		panic(err)
	}
	filePath := filepath.Join("configs", pb.Sdk_SDK_JAVA.String()+".json")
	err = os.WriteFile(filePath, []byte(javaConfig), 0600)
	if err != nil {
		panic(err)
	}

	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	os.Setenv("BEAM_SDK", pb.Sdk_SDK_JAVA.String())
	os.Setenv("APP_WORK_DIR", path)

	cacheService = local.New(context.Background())
}

func teardown() {
	err := os.RemoveAll("configs")
	if err != nil {
		panic(fmt.Errorf("error during test teardown: %s", err.Error()))
	}
	err = os.RemoveAll("executable_files")
	if err != nil {
		panic(fmt.Errorf("error during test teardown: %s", err.Error()))
	}
	os.Clearenv()
}

func Test_Process(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	appEnvs, err := environment.GetApplicationEnvsFromOsEnvs()
	if err != nil {
		panic(err)
	}
	sdkEnv, err := environment.ConfigureBeamEnvs(appEnvs.WorkingDir())
	if err != nil {
		panic(err)
	}

	type args struct {
		ctx        context.Context
		appEnv     *environment.ApplicationEnvs
		sdkEnv     *environment.BeamEnvs
		pipelineId uuid.UUID
	}
	tests := []struct {
		name                  string
		createExecFile        bool
		code                  string
		cancelFunc            bool
		expectedStatus        pb.Status
		expectedRunOutput     interface{}
		expectedRunError      interface{}
		expectedCompileOutput interface{}
		args                  args
	}{
		{
			// Test case with calling processCode method with small timeout.
			// As a result status into cache should be set as Status_STATUS_RUN_TIMEOUT.
			name:                  "small pipeline execution timeout",
			createExecFile:        false,
			code:                  "",
			cancelFunc:            false,
			expectedStatus:        pb.Status_STATUS_RUN_TIMEOUT,
			expectedCompileOutput: nil,
			expectedRunOutput:     nil,
			expectedRunError:      nil,
			args: args{
				ctx:        context.Background(),
				appEnv:     &environment.ApplicationEnvs{},
				sdkEnv:     sdkEnv,
				pipelineId: uuid.New(),
			},
		},
		{
			// Test case with calling processCode method without preparing files with code.
			// As a result status into cache should be set as Status_STATUS_VALIDATION_ERROR.
			name:                  "validation failed",
			createExecFile:        false,
			code:                  "",
			cancelFunc:            false,
			expectedStatus:        pb.Status_STATUS_VALIDATION_ERROR,
			expectedCompileOutput: nil,
			expectedRunOutput:     nil,
			expectedRunError:      nil,
			args: args{
				ctx:        context.Background(),
				appEnv:     appEnvs,
				sdkEnv:     sdkEnv,
				pipelineId: uuid.New(),
			},
		},
		{
			// Test case with calling processCode method with incorrect code.
			// As a result status into cache should be set as Status_STATUS_COMPILE_ERROR.
			name:                  "compilation failed",
			createExecFile:        true,
			code:                  "MOCK_CODE",
			cancelFunc:            false,
			expectedStatus:        pb.Status_STATUS_COMPILE_ERROR,
			expectedCompileOutput: "error: exit status 1, output: %s:1: error: reached end of file while parsing\nMOCK_CODE\n^\n1 error\n",
			expectedRunOutput:     nil,
			expectedRunError:      nil,
			args: args{
				ctx:        context.Background(),
				appEnv:     appEnvs,
				sdkEnv:     sdkEnv,
				pipelineId: uuid.New(),
			},
		},
		{
			// Test case with calling processCode method with incorrect logic into code.
			// As a result status into cache should be set as Status_STATUS_RUN_ERROR.
			name:                  "run failed",
			createExecFile:        true,
			code:                  "class HelloWorld {\n    public static void main(String[] args) {\n        System.out.println(1/0);\n    }\n}",
			cancelFunc:            false,
			expectedStatus:        pb.Status_STATUS_RUN_ERROR,
			expectedCompileOutput: "",
			expectedRunOutput:     "",
			expectedRunError:      "error: exit status 1, output: Exception in thread \"main\" java.lang.ArithmeticException: / by zero\n\tat HelloWorld.main(%s.java:3)\n",
			args: args{
				ctx:        context.Background(),
				appEnv:     appEnvs,
				sdkEnv:     sdkEnv,
				pipelineId: uuid.New(),
			},
		},
		{
			// Test case with calling processCode with canceling code processing.
			// As a result status into cache should be set as Status_STATUS_CANCELED.
			name:                  "cancel",
			createExecFile:        true,
			code:                  "class HelloWorld {\n    public static void main(String[] args) {\n        while(true){}\n    }\n}",
			cancelFunc:            true,
			expectedStatus:        pb.Status_STATUS_CANCELED,
			expectedCompileOutput: "",
			expectedRunOutput:     "",
			args: args{
				ctx:        context.Background(),
				appEnv:     appEnvs,
				sdkEnv:     sdkEnv,
				pipelineId: uuid.New(),
			},
		},
		{
			// Test case with calling processCode without any error cases.
			// As a result status into cache should be set as Status_STATUS_FINISHED.
			name:                  "processing complete successfully",
			createExecFile:        true,
			cancelFunc:            false,
			code:                  "class HelloWorld {\n    public static void main(String[] args) {\n        System.out.println(\"Hello world!\");\n    }\n}",
			expectedStatus:        pb.Status_STATUS_FINISHED,
			expectedCompileOutput: "",
			expectedRunOutput:     "Hello world!\n",
			expectedRunError:      nil,
			args: args{
				ctx:        context.Background(),
				appEnv:     appEnvs,
				sdkEnv:     sdkEnv,
				pipelineId: uuid.New(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc, _ := fs_tool.NewLifeCycle(pb.Sdk_SDK_JAVA, tt.args.pipelineId, os.Getenv("APP_WORK_DIR"))
			filePath := lc.GetAbsoluteExecutableFilePath()
			workingDir := lc.GetAbsoluteExecutableFilesFolderPath()

			exec := executors.NewExecutorBuilder().
				WithValidator().
				WithSdkValidators(validators.GetJavaValidators(filePath)).
				WithCompiler().
				WithCommand(sdkEnv.ExecutorConfig.CompileCmd).
				WithArgs(sdkEnv.ExecutorConfig.CompileArgs).
				WithFileName(filePath).
				WithWorkingDir(workingDir)

			err := lc.CreateFolders()
			if err != nil {
				t.Fatalf("error during prepare folders: %s", err.Error())
			}
			if tt.createExecFile {
				_, _ = lc.CreateExecutableFile(tt.code)
			}

			if tt.cancelFunc {
				go func(ctx context.Context, pipelineId uuid.UUID) {
					// to imitate behavior of cancellation
					time.Sleep(5 * time.Second)
					cacheService.SetValue(ctx, pipelineId, cache.Canceled, true)
				}(tt.args.ctx, tt.args.pipelineId)
			}
			Process(tt.args.ctx, cacheService, lc, exec, tt.args.pipelineId, tt.args.appEnv, tt.args.sdkEnv)

			status, _ := cacheService.GetValue(tt.args.ctx, tt.args.pipelineId, cache.Status)
			if !reflect.DeepEqual(status, tt.expectedStatus) {
				t.Errorf("processCode() set status: %s, but expectes: %s", status, tt.expectedStatus)
			}

			compileOutput, _ := cacheService.GetValue(tt.args.ctx, tt.args.pipelineId, cache.CompileOutput)
			if tt.expectedCompileOutput != nil && strings.Contains(tt.expectedCompileOutput.(string), "%s") {
				tt.expectedCompileOutput = fmt.Sprintf(tt.expectedCompileOutput.(string), lc.GetAbsoluteExecutableFilePath())
			}
			if !reflect.DeepEqual(compileOutput, tt.expectedCompileOutput) {
				t.Errorf("processCode() set compileOutput: %s, but expectes: %s", compileOutput, tt.expectedCompileOutput)
			}

			runOutput, _ := cacheService.GetValue(tt.args.ctx, tt.args.pipelineId, cache.RunOutput)
			if !reflect.DeepEqual(runOutput, tt.expectedRunOutput) {
				t.Errorf("processCode() set runOutput: %s, but expectes: %s", runOutput, tt.expectedRunOutput)
			}

			runError, _ := cacheService.GetValue(tt.args.ctx, tt.args.pipelineId, cache.RunError)
			if tt.expectedRunError != nil && strings.Contains(tt.expectedRunError.(string), "%s") {
				tt.expectedRunError = fmt.Sprintf(tt.expectedRunError.(string), tt.args.pipelineId)
			}
			if !reflect.DeepEqual(runError, tt.expectedRunError) {
				t.Errorf("processCode() set runError: %s, but expectes: %s", runError, tt.expectedRunError)
			}

			// remove
			path := os.Getenv("APP_WORK_DIR") + "/executable_files"
			os.RemoveAll(path)
		})
	}
}

func TestGetProcessingOutput(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	pipelineId := uuid.New()
	incorrectConvertPipelineId := uuid.New()
	err := cacheService.SetValue(context.Background(), pipelineId, cache.RunOutput, "MOCK_RUN_OUTPUT")
	if err != nil {
		panic(err)
	}
	err = cacheService.SetValue(context.Background(), incorrectConvertPipelineId, cache.RunOutput, cache.RunOutput)
	if err != nil {
		panic(err)
	}

	type args struct {
		ctx          context.Context
		cacheService cache.Cache
		key          uuid.UUID
		subKey       cache.SubKey
		errorTitle   string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			// Test case with calling GetProcessingOutput with pipelineId which doesn't contain run output.
			// As a result, want to receive an error.
			name: "get run output with incorrect pipelineId",
			args: args{
				ctx:          context.Background(),
				cacheService: cacheService,
				key:          uuid.New(),
				subKey:       cache.RunOutput,
				errorTitle:   "",
			},
			want:    "",
			wantErr: true,
		},
		{
			// Test case with calling GetProcessingOutput with pipelineId which contains incorrect run output.
			// As a result, want to receive an error.
			name: "get run output with incorrect run output",
			args: args{
				ctx:          context.Background(),
				cacheService: cacheService,
				key:          incorrectConvertPipelineId,
				subKey:       cache.RunOutput,
				errorTitle:   "",
			},
			want:    "",
			wantErr: true,
		},
		{
			// Test case with calling GetProcessingOutput with pipelineId which contains run output.
			// As a result, want to receive an expected string.
			name: "get run output with correct pipelineId",
			args: args{
				ctx:          context.Background(),
				cacheService: cacheService,
				key:          pipelineId,
				subKey:       cache.RunOutput,
				errorTitle:   "",
			},
			want:    "MOCK_RUN_OUTPUT",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetProcessingOutput(tt.args.ctx, tt.args.cacheService, tt.args.key, tt.args.subKey, tt.args.errorTitle)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetProcessingOutput() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetProcessingOutput() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetProcessingStatus(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	pipelineId := uuid.New()
	incorrectConvertPipelineId := uuid.New()
	err := cacheService.SetValue(context.Background(), pipelineId, cache.Status, pb.Status_STATUS_FINISHED)
	if err != nil {
		panic(err)
	}
	err = cacheService.SetValue(context.Background(), incorrectConvertPipelineId, cache.Status, "MOCK_STATUS")
	if err != nil {
		panic(err)
	}

	type args struct {
		ctx          context.Context
		cacheService cache.Cache
		key          uuid.UUID
		errorTitle   string
	}
	tests := []struct {
		name    string
		args    args
		want    pb.Status
		wantErr bool
	}{
		{
			// Test case with calling GetProcessingStatus with pipelineId which doesn't contain status.
			// As a result, want to receive an error.
			name: "get status with incorrect pipelineId",
			args: args{
				ctx:          context.Background(),
				cacheService: cacheService,
				key:          uuid.New(),
				errorTitle:   "",
			},
			want:    pb.Status_STATUS_UNSPECIFIED,
			wantErr: true,
		},
		{
			// Test case with calling GetProcessingStatus with pipelineId which contains incorrect status value in cache.
			// As a result, want to receive an error.
			name: "get status with incorrect cache value",
			args: args{
				ctx:          context.Background(),
				cacheService: cacheService,
				key:          incorrectConvertPipelineId,
				errorTitle:   "",
			},
			want:    pb.Status_STATUS_UNSPECIFIED,
			wantErr: true,
		},
		{
			// Test case with calling GetProcessingStatus with pipelineId which contains status.
			// As a result, want to receive an expected status.
			name: "get status with correct pipelineId",
			args: args{
				ctx:          context.Background(),
				cacheService: cacheService,
				key:          pipelineId,
				errorTitle:   "",
			},
			want:    pb.Status_STATUS_FINISHED,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetProcessingStatus(tt.args.ctx, tt.args.cacheService, tt.args.key, tt.args.errorTitle)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetProcessingStatus() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetProcessingStatus() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetRunOutputLastIndex(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	pipelineId := uuid.New()
	incorrectConvertPipelineId := uuid.New()
	err := cacheService.SetValue(context.Background(), pipelineId, cache.RunOutputIndex, 2)
	if err != nil {
		panic(err)
	}
	err = cacheService.SetValue(context.Background(), incorrectConvertPipelineId, cache.RunOutputIndex, "MOCK_LAST_INDEX")
	if err != nil {
		panic(err)
	}

	type args struct {
		ctx          context.Context
		cacheService cache.Cache
		key          uuid.UUID
		errorTitle   string
	}
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr bool
	}{
		{
			// Test case with calling GetRunOutputLastIndex with pipelineId which doesn't contain status.
			// As a result, want to receive an error.
			name: "get last index with incorrect pipelineId",
			args: args{
				ctx:          context.Background(),
				cacheService: cacheService,
				key:          uuid.New(),
				errorTitle:   "",
			},
			want:    0,
			wantErr: true,
		},
		{
			// Test case with calling GetRunOutputLastIndex with pipelineId which contains incorrect status value in cache.
			// As a result, want to receive an error.
			name: "get last index with incorrect cache value",
			args: args{
				ctx:          context.Background(),
				cacheService: cacheService,
				key:          incorrectConvertPipelineId,
				errorTitle:   "",
			},
			want:    0,
			wantErr: true,
		},
		{
			// Test case with calling GetRunOutputLastIndex with pipelineId which contains status.
			// As a result, want to receive an expected last index.
			name: "get last index with correct pipelineId",
			args: args{
				ctx:          context.Background(),
				cacheService: cacheService,
				key:          pipelineId,
				errorTitle:   "",
			},
			want:    2,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetRunOutputLastIndex(tt.args.ctx, tt.args.cacheService, tt.args.key, tt.args.errorTitle)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetRunOutputLastIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetRunOutputLastIndex() got = %v, want %v", got, tt.want)
			}
		})
	}
}
