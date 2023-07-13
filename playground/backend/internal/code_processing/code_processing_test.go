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
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redismock/v8"
	"github.com/google/uuid"
	"go.uber.org/goleak"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/cache"
	"beam.apache.org/playground/backend/internal/cache/local"
	"beam.apache.org/playground/backend/internal/cache/redis"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/executors"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"beam.apache.org/playground/backend/internal/utils"
	"beam.apache.org/playground/backend/internal/validators"
)

const (
	javaConfig      = "{\n  \"compile_cmd\": \"javac\",\n  \"run_cmd\": \"java\",\n  \"test_cmd\": \"java\",\n  \"compile_args\": [\n    \"-d\",\n    \"bin\",\n    \"-parameters\",\n    \"-classpath\"\n  ],\n  \"run_args\": [\n    \"-cp\",\n    \"../bin:\"\n  ],\n  \"test_args\": [\n    \"-cp\",\n    \"bin:\",\n    \"JUnit\"\n  ]\n}"
	pythonConfig    = "{\n  \"compile_cmd\": \"\",\n  \"run_cmd\": \"python3\",\n  \"compile_args\": [],\n  \"run_args\": []\n}"
	goConfig        = "{\n  \"compile_cmd\": \"go\",\n  \"run_cmd\": \"\",\n  \"compile_args\": [\n    \"build\",\n    \"-o\",\n    \"bin\"\n  ],\n  \"run_args\": [\n  ]\n}"
	pipelinesFolder = "executable_files"
	configFolder    = "configs"
	resourcesFolder = "resources"
	helloWordPython = "if __name__ == \"__main__\":\n    print(\"Hello world!\")\n"
	helloWordGo     = "package main\nimport \"fmt\"\nfunc main() {\n    fmt.Println(\"hello world\")\n}\n"
	helloWordJava   = "class HelloWorld {\n    public static void main(String[] args) {\n        System.out.println(\"Hello world!\");\n    }\n}"
	graphFilePath   = "resources/graph.dot"
	jsonExtension   = ".json"
)

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
	err := os.MkdirAll(configFolder, fs.ModePerm)
	if err != nil {
		panic(err)
	}
	filePath := filepath.Join(configFolder, fmt.Sprintf("%s%s", pb.Sdk_SDK_JAVA.String(), jsonExtension))
	err = os.WriteFile(filePath, []byte(javaConfig), 0600)
	if err != nil {
		panic(err)
	}

	// create dir with graph file
	err = os.MkdirAll(resourcesFolder, fs.ModePerm)
	if err != nil {
		panic(err)
	}
	err = os.WriteFile(graphFilePath, []byte("graph"), 0600)
	if err != nil {
		panic(err)
	}

	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	err = os.Setenv("BEAM_SDK", pb.Sdk_SDK_JAVA.String())
	if err != nil {
		panic(err)
	}
	err = os.Setenv("APP_WORK_DIR", path)
	if err != nil {
		panic(err)
	}

	cacheService = local.New(context.Background())
}

func teardown() {
	err := os.RemoveAll(configFolder)
	if err != nil {
		panic(fmt.Errorf("error during test teardown: %s", err.Error()))
	}
	err = os.RemoveAll(pipelinesFolder)
	if err != nil {
		panic(fmt.Errorf("error during test teardown: %s", err.Error()))
	}
	err = os.RemoveAll(resourcesFolder)
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
	sdkJavaEnv, err := environment.ConfigureBeamEnvs(appEnvs.WorkingDir())
	if err != nil {
		panic(err)
	}
	sdkGoEnv := *sdkJavaEnv
	sdkGoEnv.ApacheBeamSdk = pb.Sdk_SDK_GO
	incorrectGoHelloWord := "package main\nimport \"fmt\"\nfunc main() {\n    fmt.Println(\"hello world\").\n}\n"
	type args struct {
		ctx             context.Context
		appEnv          *environment.ApplicationEnvs
		sdkEnv          *environment.BeamEnvs
		pipelineId      uuid.UUID
		pipelineOptions string
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
			name:                  "Small pipeline execution timeout",
			createExecFile:        false,
			code:                  "",
			cancelFunc:            false,
			expectedStatus:        pb.Status_STATUS_RUN_TIMEOUT,
			expectedCompileOutput: nil,
			expectedRunOutput:     nil,
			expectedRunError:      nil,
			args: args{
				ctx:             context.Background(),
				appEnv:          &environment.ApplicationEnvs{},
				sdkEnv:          sdkJavaEnv,
				pipelineId:      uuid.New(),
				pipelineOptions: "",
			},
		},
		{
			// Test case with calling processCode method without preparing files with code.
			// As a result status into cache should be set as Status_STATUS_VALIDATION_ERROR.
			name:                  "Validation failed",
			createExecFile:        false,
			code:                  "",
			cancelFunc:            false,
			expectedStatus:        pb.Status_STATUS_VALIDATION_ERROR,
			expectedCompileOutput: nil,
			expectedRunOutput:     nil,
			expectedRunError:      nil,
			args: args{
				ctx:             context.Background(),
				appEnv:          appEnvs,
				sdkEnv:          sdkJavaEnv,
				pipelineId:      uuid.New(),
				pipelineOptions: "",
			},
		},
		{
			// Test case with calling processCode method with incorrect code.
			// As a result status into cache should be set as Status_STATUS_COMPILE_ERROR.
			name:                  "Compilation failed",
			createExecFile:        true,
			code:                  "MOCK_CODE",
			cancelFunc:            false,
			expectedStatus:        pb.Status_STATUS_COMPILE_ERROR,
			expectedCompileOutput: "error: exit status 1\noutput: %s:1: error: reached end of file while parsing\nMOCK_CODE\n^\n1 error\n",
			expectedRunOutput:     nil,
			expectedRunError:      nil,
			args: args{
				ctx:             context.Background(),
				appEnv:          appEnvs,
				sdkEnv:          sdkJavaEnv,
				pipelineId:      uuid.New(),
				pipelineOptions: "",
			},
		},
		{
			// Test case with calling processCode method with incorrect logic into code.
			// As a result status into cache should be set as Status_STATUS_RUN_ERROR.
			name:                  "Run failed",
			createExecFile:        true,
			code:                  "class HelloWorld {\n    public static void main(String[] args) {\n        System.out.println(1/0);\n    }\n}",
			cancelFunc:            false,
			expectedStatus:        pb.Status_STATUS_RUN_ERROR,
			expectedCompileOutput: "",
			expectedRunOutput:     "",
			expectedRunError:      "error: exit status 1\noutput: Exception in thread \"main\" java.lang.ArithmeticException: / by zero\n\tat HelloWorld.main(%s.java:3)\n",
			args: args{
				ctx:             context.Background(),
				appEnv:          appEnvs,
				sdkEnv:          sdkJavaEnv,
				pipelineId:      uuid.New(),
				pipelineOptions: "",
			},
		},
		{
			// Test case with calling processCode with canceling code processing.
			// As a result status into cache should be set as Status_STATUS_CANCELED.
			name:                  "Cancel",
			createExecFile:        true,
			code:                  "class HelloWorld {\n    public static void main(String[] args) {\n        while(true){}\n    }\n}",
			cancelFunc:            true,
			expectedStatus:        pb.Status_STATUS_CANCELED,
			expectedCompileOutput: "",
			expectedRunOutput:     "",
			expectedRunError:      "",
			args: args{
				ctx:             context.Background(),
				appEnv:          appEnvs,
				sdkEnv:          sdkJavaEnv,
				pipelineId:      uuid.New(),
				pipelineOptions: "",
			},
		},
		{
			// Test case with calling processCode without any error cases.
			// As a result status into cache should be set as Status_STATUS_FINISHED.
			name:                  "Processing complete successfully on java sdk",
			createExecFile:        true,
			cancelFunc:            false,
			code:                  helloWordJava,
			expectedStatus:        pb.Status_STATUS_FINISHED,
			expectedCompileOutput: "",
			expectedRunOutput:     "Hello world!\n",
			expectedRunError:      "",
			args: args{
				ctx:             context.Background(),
				appEnv:          appEnvs,
				sdkEnv:          sdkJavaEnv,
				pipelineId:      uuid.New(),
				pipelineOptions: "",
			},
		},
		{
			// Test case with calling processCode method with incorrect go code.
			// As a result status into cache should be set as Status_STATUS_PREPARATION_ERROR.
			name:                  "Prepare step failed",
			createExecFile:        true,
			code:                  incorrectGoHelloWord,
			cancelFunc:            false,
			expectedStatus:        pb.Status_STATUS_PREPARATION_ERROR,
			expectedCompileOutput: nil,
			expectedRunOutput:     nil,
			expectedRunError:      nil,
			args: args{
				ctx:             context.Background(),
				appEnv:          appEnvs,
				sdkEnv:          &sdkGoEnv,
				pipelineId:      uuid.New(),
				pipelineOptions: "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc, _ := fs_tool.NewLifeCycle(pb.Sdk_SDK_JAVA, tt.args.pipelineId, filepath.Join(os.Getenv("APP_WORK_DIR"), pipelinesFolder))
			err := lc.CreateFolders()
			if err != nil {
				t.Fatalf("error during prepare folders: %s", err.Error())
			}

			sources := []entity.FileEntity{{Name: "main.java", Content: tt.code, IsMain: true}}
			if tt.createExecFile {
				_ = lc.CreateSourceCodeFiles(sources)
			}
			if err = utils.SetToCache(cacheService, tt.args.pipelineId, cache.Canceled, false); err != nil {
				t.Fatal("error during set cancel flag to cache")
			}
			if tt.cancelFunc {
				go func(ctx context.Context, pipelineId uuid.UUID) {
					// to imitate behavior of cancellation
					time.Sleep(5 * time.Second)
					_ = cacheService.SetValue(ctx, pipelineId, cache.Canceled, true)
				}(tt.args.ctx, tt.args.pipelineId)
			}
			Process(tt.args.ctx, cacheService, lc, tt.args.pipelineId, tt.args.appEnv, tt.args.sdkEnv, tt.args.pipelineOptions)

			status, _ := cacheService.GetValue(tt.args.ctx, tt.args.pipelineId, cache.Status)
			if !reflect.DeepEqual(status, tt.expectedStatus) {
				t.Errorf("processCode() set status: %s, but expectes: %s", status, tt.expectedStatus)
			}

			compileOutput, _ := cacheService.GetValue(tt.args.ctx, tt.args.pipelineId, cache.CompileOutput)
			if tt.expectedCompileOutput != nil && strings.Contains(tt.expectedCompileOutput.(string), "%s") {
				tt.expectedCompileOutput = fmt.Sprintf(tt.expectedCompileOutput.(string), lc.Paths.AbsoluteSourceFilePath)
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
			name: "Get run output with incorrect pipelineId",
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
			name: "Get run output with incorrect run output",
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
			name: "Get run output with correct pipelineId",
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
			name: "Get status with incorrect pipelineId",
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
			name: "Get status with incorrect cache value",
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
			name: "Get status with correct pipelineId",
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

func TestGetLastIndex(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	pipelineId := uuid.New()
	incorrectConvertPipelineId := uuid.New()
	err := cacheService.SetValue(context.Background(), pipelineId, cache.RunOutputIndex, 2)
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
		want    int
		wantErr bool
	}{
		{
			// Test case with calling GetLastIndex with pipelineId which doesn't contain last index.
			// As a result, want to receive an error.
			name: "Get last index with incorrect pipelineId",
			args: args{
				ctx:          context.Background(),
				cacheService: cacheService,
				key:          uuid.New(),
				subKey:       cache.RunOutputIndex,
				errorTitle:   "",
			},
			want:    0,
			wantErr: true,
		},
		{
			// Test case with calling GetLastIndex with pipelineId which contains incorrect status value in cache.
			// As a result, want to receive an error.
			name: "Get last index with incorrect cache value",
			args: args{
				ctx:          context.Background(),
				cacheService: cacheService,
				key:          incorrectConvertPipelineId,
				subKey:       cache.RunOutputIndex,
				errorTitle:   "",
			},
			want:    0,
			wantErr: true,
		},
		{
			// Test case with calling GetLastIndex with pipelineId which contains last index.
			// As a result, want to receive an expected last index.
			name: "Get last index with correct pipelineId",
			args: args{
				ctx:          context.Background(),
				cacheService: cacheService,
				key:          pipelineId,
				subKey:       cache.RunOutputIndex,
				errorTitle:   "",
			},
			want:    2,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetLastIndex(tt.args.ctx, tt.args.cacheService, tt.args.key, tt.args.subKey, tt.args.errorTitle)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetLastIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetLastIndex() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getRunOrTestCmd(t *testing.T) {
	runEx := executors.NewExecutorBuilder().
		WithRunner().
		WithCommand("runCommand").
		WithArgs([]string{"arg1"}).
		WithPipelineOptions([]string{""}).
		Build()

	testEx := executors.NewExecutorBuilder().
		WithTestRunner().
		WithCommand("testCommand").
		WithArgs([]string{"arg1"}).
		Build()

	wantRunExec := exec.CommandContext(context.Background(), "runCommand", "arg1")
	wantTestExec := exec.CommandContext(context.Background(), "testCommand", "arg1")

	type args struct {
		isUnitTest     bool
		executor       *executors.Executor
		ctxWithTimeout context.Context
	}

	tests := []struct {
		name string
		args args
		want *exec.Cmd
	}{
		{
			//Get cmd objects with set run executor
			name: "Get run cmd",
			args: args{
				isUnitTest:     false,
				executor:       &runEx,
				ctxWithTimeout: context.Background(),
			},
			want: wantRunExec,
		},
		{
			//Get cmd objects with set test executor
			name: "Get test cmd",
			args: args{
				isUnitTest:     true,
				executor:       &testEx,
				ctxWithTimeout: context.Background(),
			},
			want: wantTestExec,
		},
	}

	execComparer := cmp.Comparer(func(a exec.Cmd, b exec.Cmd) bool {
		return a.Path == b.Path &&
			cmp.Equal(a.Args, b.Args) &&
			cmp.Equal(a.Env, b.Env) &&
			a.Dir == b.Dir
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getExecuteCmd(tt.args.isUnitTest, tt.args.executor, tt.args.ctxWithTimeout); !cmp.Equal(got, tt.want, execComparer) {
				t.Errorf("getExecuteCmd() = '%v', want '%v', diff = %v", got, tt.want, cmp.Diff(got, tt.want, execComparer))
			}
		})
	}
}

func getSdkEnv(sdk pb.Sdk) (*environment.BeamEnvs, error) {
	setupSDK(sdk)
	appEnvs, err := environment.GetApplicationEnvsFromOsEnvs()
	if err != nil {
		return nil, err
	}
	sdkEnv, err := environment.ConfigureBeamEnvs(appEnvs.WorkingDir())
	if err != nil {
		return nil, err
	}
	return sdkEnv, nil
}

func setupSDK(sdk pb.Sdk) {
	err := os.MkdirAll(configFolder, fs.ModePerm)
	if err != nil {
		panic(err)
	}
	filePath := filepath.Join(configFolder, fmt.Sprintf("%s%s", sdk.String(), jsonExtension))
	switch sdk {
	case pb.Sdk_SDK_JAVA:
		err = os.WriteFile(filePath, []byte(javaConfig), 0600)
	case pb.Sdk_SDK_PYTHON:
		err = os.WriteFile(filePath, []byte(pythonConfig), 0600)
	case pb.Sdk_SDK_GO:
		err = os.WriteFile(filePath, []byte(goConfig), 0600)
	}
	if err != nil {
		panic(err)
	}

	err = os.Setenv("BEAM_SDK", sdk.String())
	if err != nil {
		panic(err)
	}
	err = os.Setenv("APP_WORK_DIR", "")
	if err != nil {
		panic(err)
	}
	err = os.Setenv("PREPARED_MOD_DIR", "")
	if err != nil {
		panic(err)
	}

	cacheService = local.New(context.Background())
}

func teardownBenchmarks() {
	err := os.RemoveAll(configFolder)
	if err != nil {
		panic(fmt.Errorf("error during test teardown: %s", err.Error()))
	}
	err = os.RemoveAll(pipelinesFolder)
	if err != nil {
		panic(fmt.Errorf("error during test teardown: %s", err.Error()))
	}
}

func prepareFiles(b *testing.B, pipelineId uuid.UUID, code string, sdk pb.Sdk) *fs_tool.LifeCycle {
	lc, err := fs_tool.NewLifeCycle(sdk, pipelineId, pipelinesFolder)
	if err != nil {
		b.Fatalf("error during initializse lc: %s", err.Error())
	}
	err = lc.CreateFolders()
	if err != nil {
		b.Fatalf("error during prepare folders: %s", err.Error())
	}
	sources := []entity.FileEntity{{Name: "main.java", Content: code, IsMain: true}}
	err = lc.CreateSourceCodeFiles(sources)
	if err != nil {
		b.Fatalf("error during prepare source code file: %s", err.Error())
	}
	return lc
}

func Benchmark_ProcessJava(b *testing.B) {
	setupSDK(pb.Sdk_SDK_JAVA)
	defer teardownBenchmarks()

	appEnv, err := environment.GetApplicationEnvsFromOsEnvs()
	if err != nil {
		b.Fatalf("error during preparing appEnv: %s", err)
	}
	sdkEnv, err := environment.ConfigureBeamEnvs(appEnv.WorkingDir())
	if err != nil {
		b.Fatalf("error during preparing sdkEnv: %s", err)
	}

	ctx := context.Background()
	code := "class HelloWorld {\n    public static void main(String[] args) {\n        System.out.println(\"Hello world!\");\n    }\n}"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		pipelineId := uuid.New()
		lc := prepareFiles(b, pipelineId, code, pb.Sdk_SDK_JAVA)
		if err = utils.SetToCache(cacheService, pipelineId, cache.Canceled, false); err != nil {
			b.Fatal("error during set cancel flag to cache")
		}
		b.StartTimer()

		Process(ctx, cacheService, lc, pipelineId, appEnv, sdkEnv, "")
	}
}

func Benchmark_ProcessPython(b *testing.B) {
	setupSDK(pb.Sdk_SDK_PYTHON)
	defer teardownBenchmarks()

	appEnv, err := environment.GetApplicationEnvsFromOsEnvs()
	if err != nil {
		b.Fatalf("error during preparing appEnv: %s", err)
	}
	sdkEnv, err := environment.ConfigureBeamEnvs(appEnv.WorkingDir())
	if err != nil {
		b.Fatalf("error during preparing sdkEnv: %s", err)
	}

	ctx := context.Background()
	wordCountCode := "import argparse\nimport logging\nimport re\n\nimport apache_beam as beam\nfrom apache_beam.io import ReadFromText\nfrom apache_beam.io import WriteToText\nfrom apache_beam.options.pipeline_options import PipelineOptions\nfrom apache_beam.options.pipeline_options import SetupOptions\n\n\nclass WordExtractingDoFn(beam.DoFn):\n  \"\"\"Parse each line of input text into words.\"\"\"\n  def process(self, element):\n    \"\"\"Returns an iterator over the words of this element.\n\n    The element is a line of text.  If the line is blank, note that, too.\n\n    Args:\n      element: the element being processed\n\n    Returns:\n      The processed element.\n    \"\"\"\n    return re.findall(r'[\\w\\']+', element, re.UNICODE)\n\n\ndef run(argv=None, save_main_session=True):\n  \"\"\"Main entry point; defines and runs the wordcount pipeline.\"\"\"\n  parser = argparse.ArgumentParser()\n  parser.add_argument(\n      '--input',\n      dest='input',\n      default='gs://dataflow-samples/shakespeare/kinglear.txt',\n      help='Input file to process.')\n  parser.add_argument(\n      '--output',\n      dest='output',\n      required=True,\n      help='Output file to write results to.')\n  known_args, pipeline_args = parser.parse_known_args(argv)\n\n  # We use the save_main_session option because one or more DoFn's in this\n  # workflow rely on global context (e.g., a module imported at module level).\n  pipeline_options = PipelineOptions(pipeline_args)\n  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session\n\n  # The pipeline will be run on exiting the with block.\n  with beam.Pipeline(options=pipeline_options) as p:\n\n    # Read the text file[pattern] into a PCollection.\n    lines = p | 'Read' >> ReadFromText(known_args.input)\n\n    counts = (\n        lines\n        | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))\n        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))\n        | 'GroupAndSum' >> beam.CombinePerKey(sum))\n\n    # Format the counts into a PCollection of strings.\n    def format_result(word, count):\n      return '%s: %d' % (word, count)\n\n    output = counts | 'Format' >> beam.MapTuple(format_result)\n\n    # Write the output using a \"Write\" transform that has side effects.\n    # pylint: disable=expression-not-assigned\n    output | 'Write' >> WriteToText(known_args.output)\n\n\nif __name__ == '__main__':\n  logging.getLogger().setLevel(logging.INFO)\n  run()"
	pipelineOptions := "--output t.txt"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		pipelineId := uuid.New()
		lc := prepareFiles(b, pipelineId, wordCountCode, pb.Sdk_SDK_PYTHON)
		if err = utils.SetToCache(cacheService, pipelineId, cache.Canceled, false); err != nil {
			b.Fatal("error during set cancel flag to cache")
		}
		b.StartTimer()

		Process(ctx, cacheService, lc, pipelineId, appEnv, sdkEnv, pipelineOptions)
	}
}

func Benchmark_ProcessGo(b *testing.B) {
	setupSDK(pb.Sdk_SDK_GO)
	defer teardownBenchmarks()

	appEnv, err := environment.GetApplicationEnvsFromOsEnvs()
	if err != nil {
		b.Fatalf("error during preparing appEnv: %s", err)
	}
	sdkEnv, err := environment.ConfigureBeamEnvs(appEnv.WorkingDir())
	if err != nil {
		b.Fatalf("error during preparing sdkEnv: %s", err)
	}

	ctx := context.Background()
	code := "package main\n\nimport \"fmt\"\n\nfunc main() {\n    fmt.Println(\"Hello world!\")\n}"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		pipelineId := uuid.New()
		lc := prepareFiles(b, pipelineId, code, pb.Sdk_SDK_GO)
		if err = utils.SetToCache(cacheService, pipelineId, cache.Canceled, false); err != nil {
			b.Fatal("error during set cancel flag to cache")
		}
		b.StartTimer()

		Process(ctx, cacheService, lc, pipelineId, appEnv, sdkEnv, "")
	}
}

func Benchmark_GetProcessingOutput(b *testing.B) {
	pipelineId := uuid.New()
	subKey := cache.RunOutput
	ctx := context.Background()

	err := cacheService.SetValue(ctx, pipelineId, subKey, "MOCK_RUN_OUTPUT")
	if err != nil {
		b.Fatalf("error during prepare cache value: %s", err.Error())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetProcessingOutput(ctx, cacheService, pipelineId, subKey, "")
	}
}

func Benchmark_GetProcessingStatus(b *testing.B) {
	pipelineId := uuid.New()
	subKey := cache.Status
	ctx := context.Background()

	err := cacheService.SetValue(ctx, pipelineId, subKey, pb.Status_STATUS_FINISHED)
	if err != nil {
		b.Fatalf("error during prepare cache value: %s", err.Error())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetProcessingStatus(ctx, cacheService, pipelineId, "")
	}
}

func Benchmark_GetLastIndex(b *testing.B) {
	pipelineId := uuid.New()
	subKey := cache.RunOutputIndex
	ctx := context.Background()

	err := cacheService.SetValue(ctx, pipelineId, subKey, 5)
	if err != nil {
		b.Fatalf("error during prepare cache value: %s", err.Error())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetLastIndex(ctx, cacheService, pipelineId, subKey, "")
	}
}

func Test_validateStep(t *testing.T) {
	javaSdkEnv, err := getSdkEnv(pb.Sdk_SDK_JAVA)
	if err != nil {
		panic(err)
	}
	incorrectSdkEnv := &environment.BeamEnvs{
		ApacheBeamSdk:  pb.Sdk_SDK_UNSPECIFIED,
		ExecutorConfig: nil,
	}
	type args struct {
		ctx                  context.Context
		cacheService         cache.Cache
		pipelineId           uuid.UUID
		sdkEnv               *environment.BeamEnvs
		pipelineLifeCycleCtx context.Context
		validationResults    *sync.Map
	}
	tests := []struct {
		name string
		args args
		want int
		code string
	}{
		{
			name: "Test validation step by checking number of validators",
			args: args{
				ctx:                  context.Background(),
				cacheService:         cacheService,
				pipelineId:           uuid.New(),
				sdkEnv:               javaSdkEnv,
				pipelineLifeCycleCtx: context.Background(),
				validationResults:    &sync.Map{},
			},
			want: 3,
			code: helloWordJava,
		},
		{
			name: "Test validation step with incorrect sdkEnv",
			args: args{
				ctx:                  context.Background(),
				cacheService:         cacheService,
				pipelineId:           uuid.New(),
				sdkEnv:               incorrectSdkEnv,
				pipelineLifeCycleCtx: context.Background(),
				validationResults:    &sync.Map{},
			},
			want: 0,
			code: helloWordJava,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc, _ := fs_tool.NewLifeCycle(pb.Sdk_SDK_JAVA, tt.args.pipelineId, filepath.Join(os.Getenv("APP_WORK_DIR"), pipelinesFolder))
			err := lc.CreateFolders()
			if err != nil {
				t.Fatalf("error during prepare folders: %s", err.Error())
			}
			sources := []entity.FileEntity{{Name: "main.java", Content: tt.code, IsMain: true}}
			_ = lc.CreateSourceCodeFiles(sources)
			err = validateStep(tt.args.pipelineLifeCycleCtx, tt.args.cacheService, &lc.Paths, tt.args.pipelineId, tt.args.sdkEnv, tt.args.validationResults)
			got := syncMapLen(tt.args.validationResults)
			if err != nil && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("validateStep() = %d, want %d", got, tt.want)
			}
		})
	}
}

func Test_prepareStep(t *testing.T) {
	javaSdkEnv, err := getSdkEnv(pb.Sdk_SDK_JAVA)
	if err != nil {
		panic(err)
	}
	incorrectSdkEnv := &environment.BeamEnvs{
		ApacheBeamSdk:  pb.Sdk_SDK_UNSPECIFIED,
		ExecutorConfig: nil,
	}
	validationResults := sync.Map{}
	validationResults.Store(validators.UnitTestValidatorName, false)
	validationResults.Store(validators.KatasValidatorName, false)
	pipelineLifeCycleCtx, cancel := context.WithTimeout(context.Background(), 1)
	defer cancel()
	type args struct {
		ctx                  context.Context
		cacheService         cache.Cache
		pipelineId           uuid.UUID
		sdkEnv               *environment.BeamEnvs
		pipelineLifeCycleCtx context.Context
		validationResults    *sync.Map
	}
	tests := []struct {
		name           string
		args           args
		code           string
		expectedStatus pb.Status
	}{
		{
			name: "Test preparer step working without an error",
			args: args{
				ctx:                  context.Background(),
				cacheService:         cacheService,
				pipelineId:           uuid.New(),
				sdkEnv:               javaSdkEnv,
				pipelineLifeCycleCtx: context.Background(),
				validationResults:    &validationResults,
			},
			code:           helloWordJava,
			expectedStatus: pb.Status_STATUS_COMPILING,
		},
		{
			name: "Test preparer step working with incorrect sdkEnv",
			args: args{
				ctx:                  context.Background(),
				cacheService:         cacheService,
				pipelineId:           uuid.New(),
				sdkEnv:               incorrectSdkEnv,
				pipelineLifeCycleCtx: context.Background(),
				validationResults:    &validationResults,
			},
			code:           "",
			expectedStatus: pb.Status_STATUS_ERROR,
		},
		{
			name: "Error during expired context of the example",
			args: args{
				ctx:                  context.Background(),
				cacheService:         cacheService,
				pipelineId:           uuid.New(),
				sdkEnv:               javaSdkEnv,
				pipelineLifeCycleCtx: pipelineLifeCycleCtx,
				validationResults:    &validationResults,
			},
			code:           "",
			expectedStatus: pb.Status_STATUS_RUN_TIMEOUT,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc, _ := fs_tool.NewLifeCycle(pb.Sdk_SDK_JAVA, tt.args.pipelineId, filepath.Join(os.Getenv("APP_WORK_DIR"), pipelinesFolder))
			err := lc.CreateFolders()
			if err != nil {
				t.Fatalf("error during prepare folders: %s", err.Error())
			}
			sources := []entity.FileEntity{{Name: "main.java", Content: tt.code, IsMain: true}}
			_ = lc.CreateSourceCodeFiles(sources)
			_ = prepareStep(tt.args.pipelineLifeCycleCtx, tt.args.cacheService, &lc.Paths, tt.args.pipelineId, tt.args.sdkEnv, tt.args.validationResults, nil)
			status, _ := cacheService.GetValue(tt.args.ctx, tt.args.pipelineId, cache.Status)
			if status != tt.expectedStatus {
				t.Errorf("prepareStep: got status = %v, want %v", status, tt.expectedStatus)
			}
		})
	}
}

func Test_compileStep(t *testing.T) {
	sdkJavaEnv, err := getSdkEnv(pb.Sdk_SDK_JAVA)
	if err != nil {
		panic(err)
	}
	sdkPythonEnv, err := getSdkEnv(pb.Sdk_SDK_PYTHON)
	if err != nil {
		panic(err)
	}
	pipelineLifeCycleCtx, cancel := context.WithTimeout(context.Background(), 1)
	defer cancel()
	type args struct {
		ctx                  context.Context
		cacheService         cache.Cache
		pipelineId           uuid.UUID
		sdkEnv               *environment.BeamEnvs
		isUnitTest           bool
		pipelineLifeCycleCtx context.Context
	}
	tests := []struct {
		name           string
		args           args
		code           string
		expectedStatus pb.Status
	}{
		{
			name: "Test compilation step finishes successfully on java sdk",
			args: args{
				ctx:                  context.Background(),
				cacheService:         cacheService,
				pipelineId:           uuid.New(),
				sdkEnv:               sdkJavaEnv,
				isUnitTest:           false,
				pipelineLifeCycleCtx: context.Background(),
			},
			code:           helloWordJava,
			expectedStatus: pb.Status_STATUS_EXECUTING,
		},
		{
			name: "Test compilation step finishes successfully on python sdk",
			args: args{
				ctx:                  context.Background(),
				cacheService:         cacheService,
				pipelineId:           uuid.New(),
				sdkEnv:               sdkPythonEnv,
				isUnitTest:           false,
				pipelineLifeCycleCtx: context.Background(),
			},
			code:           helloWordPython,
			expectedStatus: pb.Status_STATUS_EXECUTING,
		},
		{
			name: "Error during expired context of the example",
			args: args{
				ctx:                  context.Background(),
				cacheService:         cacheService,
				pipelineId:           uuid.New(),
				sdkEnv:               sdkJavaEnv,
				isUnitTest:           false,
				pipelineLifeCycleCtx: pipelineLifeCycleCtx,
			},
			code:           helloWordJava,
			expectedStatus: pb.Status_STATUS_RUN_TIMEOUT,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc, _ := fs_tool.NewLifeCycle(tt.args.sdkEnv.ApacheBeamSdk, tt.args.pipelineId, filepath.Join(os.Getenv("APP_WORK_DIR"), pipelinesFolder))
			err := lc.CreateFolders()
			if err != nil {
				t.Fatalf("error during prepare folders: %s", err.Error())
			}
			sources := []entity.FileEntity{{Name: "main.java", Content: tt.code, IsMain: true}}
			_ = lc.CreateSourceCodeFiles(sources)
			_ = compileStep(tt.args.pipelineLifeCycleCtx, tt.args.cacheService, &lc.Paths, tt.args.pipelineId, tt.args.sdkEnv, tt.args.isUnitTest)
			status, _ := cacheService.GetValue(tt.args.ctx, tt.args.pipelineId, cache.Status)
			if status != tt.expectedStatus {
				t.Errorf("compileStep: got status = %v, want %v", status, tt.expectedStatus)
			}
		})
	}
}

func Test_runStep(t *testing.T) {
	sdkJavaEnv, err := getSdkEnv(pb.Sdk_SDK_JAVA)
	if err != nil {
		panic(err)
	}
	sdkPythonEnv, err := getSdkEnv(pb.Sdk_SDK_PYTHON)
	if err != nil {
		panic(err)
	}
	sdkGoEnv, err := getSdkEnv(pb.Sdk_SDK_GO)
	if err != nil {
		panic(err)
	}
	type args struct {
		ctx                  context.Context
		cacheService         cache.Cache
		pipelineId           uuid.UUID
		isUnitTest           bool
		sdkEnv               *environment.BeamEnvs
		pipelineOptions      string
		pipelineLifeCycleCtx context.Context
		createExecFile       bool
	}
	tests := []struct {
		name           string
		args           args
		code           string
		expectedStatus pb.Status
	}{
		{
			// Test case with calling runStep method on python sdk.
			// cmd.Run return error during saving output.
			// As a result, the pipeline status should be Status_STATUS_RUN_ERROR.
			name: "Test run step working on python sdk",
			args: args{
				ctx:                  context.Background(),
				cacheService:         cacheService,
				pipelineId:           uuid.New(),
				isUnitTest:           false,
				sdkEnv:               sdkPythonEnv,
				pipelineOptions:      "",
				pipelineLifeCycleCtx: context.Background(),
				createExecFile:       true,
			},
			code:           helloWordPython,
			expectedStatus: pb.Status_STATUS_RUN_ERROR,
		},
		{
			// Test case with calling runStep method on go sdk.
			// cmd.Run return error due to missing executable file.
			// As a result, the pipeline status should be Status_STATUS_RUN_ERROR.
			name: "Test run step working on go sdk",
			args: args{
				ctx:                  context.Background(),
				cacheService:         cacheService,
				pipelineId:           uuid.New(),
				isUnitTest:           true,
				sdkEnv:               sdkGoEnv,
				pipelineOptions:      "",
				pipelineLifeCycleCtx: context.Background(),
				createExecFile:       true,
			},
			code:           helloWordGo,
			expectedStatus: pb.Status_STATUS_RUN_ERROR,
		},
		{
			// Test case with calling runStep method without preparing files with code.
			// As a result, the pipeline status should be Status_STATUS_ERROR.
			name: "Test run step without preparing files with code",
			args: args{
				ctx:                  context.Background(),
				cacheService:         cacheService,
				pipelineId:           uuid.UUID{},
				isUnitTest:           true,
				sdkEnv:               sdkJavaEnv,
				pipelineOptions:      "",
				pipelineLifeCycleCtx: context.Background(),
				createExecFile:       false,
			},
			code:           helloWordJava,
			expectedStatus: pb.Status_STATUS_ERROR,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc, _ := fs_tool.NewLifeCycle(tt.args.sdkEnv.ApacheBeamSdk, tt.args.pipelineId, filepath.Join(os.Getenv("APP_WORK_DIR"), pipelinesFolder))
			if tt.args.createExecFile {
				err := lc.CreateFolders()
				if err != nil {
					t.Fatalf("error during prepare folders: %s", err.Error())
				}
				sources := []entity.FileEntity{{Name: "main.java", Content: tt.code, IsMain: true}}
				_ = lc.CreateSourceCodeFiles(sources)
			}
			_ = runStep(tt.args.pipelineLifeCycleCtx, tt.args.cacheService, &lc.Paths, tt.args.pipelineId, tt.args.isUnitTest, tt.args.sdkEnv, tt.args.pipelineOptions)
			status, _ := cacheService.GetValue(tt.args.ctx, tt.args.pipelineId, cache.Status)
			if status != tt.expectedStatus {
				t.Errorf("runStep() got status = %v, want %v", status, tt.expectedStatus)
			}
		})
	}
}

func syncMapLen(syncMap *sync.Map) int {
	length := 0
	syncMap.Range(func(_, _ interface{}) bool {
		length++
		return true
	})
	return length
}

func TestGetGraph(t *testing.T) {
	ctx := context.Background()
	pipelineId1 := uuid.New()
	graph := "GRAPH"
	err := cacheService.SetValue(ctx, pipelineId1, cache.Graph, graph)
	if err != nil {
		return
	}
	pipelineId2 := uuid.New()
	err = cacheService.SetValue(ctx, pipelineId2, cache.Graph, 1)
	if err != nil {
		return
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
		want    string
		wantErr bool
	}{
		{
			name: "Get graph when key exist in cache",
			args: args{
				ctx:          context.Background(),
				cacheService: cacheService,
				key:          pipelineId1,
				errorTitle:   "error",
			},
			want:    graph,
			wantErr: false,
		},
		{
			name: "Get graph when key doesn't exist in cache",
			args: args{
				ctx:          context.Background(),
				cacheService: cacheService,
				key:          uuid.New(),
				errorTitle:   "error",
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "Get graph when value from cache by key couldn't be converted to a string",
			args: args{
				ctx:          context.Background(),
				cacheService: cacheService,
				key:          pipelineId2,
				errorTitle:   "error",
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetGraph(tt.args.ctx, tt.args.cacheService, tt.args.key, tt.args.errorTitle)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetGraph error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetGraph got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_processSetupError(t *testing.T) {
	client, mock := redismock.NewClientMock()
	pipelineId := uuid.New()
	errorMessage := "MOCK_ERROR"
	type args struct {
		err            error
		pipelineId     uuid.UUID
		cacheService   cache.Cache
		ctxWithTimeout context.Context
	}
	tests := []struct {
		name    string
		mocks   func()
		args    args
		wantErr bool
	}{
		{
			name: "Error during HSet operation",
			mocks: func() {
				mock.ExpectHSet(pipelineId.String(), "MOCK_VALUE").SetErr(fmt.Errorf(errorMessage))
			},
			args: args{
				err:        fmt.Errorf(errorMessage),
				pipelineId: pipelineId,
				cacheService: &redis.Cache{
					Client: client,
				},
				ctxWithTimeout: nil,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mocks()
			if err := processSetupError(tt.args.err, tt.args.pipelineId, tt.args.cacheService); (err != nil) != tt.wantErr {
				t.Errorf("processSetupError() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_processErrorWithSavingOutput(t *testing.T) {
	client, mock := redismock.NewClientMock()
	pipelineId := uuid.New()
	errorMessage := "MOCK_ERROR"
	subKey := cache.RunOutput
	type args struct {
		ctx          context.Context
		err          error
		errorOutput  []byte
		pipelineId   uuid.UUID
		subKey       cache.SubKey
		cacheService cache.Cache
		errorTitle   string
		newStatus    pb.Status
	}
	tests := []struct {
		name    string
		mocks   func()
		args    args
		wantErr bool
	}{
		{
			name: "Error during HSet operation",
			mocks: func() {
				mock.ExpectHSet(pipelineId.String(), subKey).SetErr(fmt.Errorf(errorMessage))
			},
			args: args{
				ctx:          context.Background(),
				err:          fmt.Errorf(errorMessage),
				errorOutput:  nil,
				pipelineId:   pipelineId,
				subKey:       subKey,
				cacheService: &redis.Cache{Client: client},
				errorTitle:   "",
				newStatus:    0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mocks()
			if err := processErrorWithSavingOutput(tt.args.err, tt.args.errorOutput, tt.args.pipelineId, tt.args.subKey, tt.args.cacheService, tt.args.errorTitle, tt.args.newStatus); (err != nil) != tt.wantErr {
				t.Errorf("processErrorWithSavingOutput() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_processRunError(t *testing.T) {
	client, mock := redismock.NewClientMock()
	pipelineId := uuid.New()
	errorMessage := "MOCK_ERROR"
	subKey := cache.RunError
	errorChannel := make(chan error, 1)
	errorChannel <- fmt.Errorf(errorMessage)
	type args struct {
		ctx                   context.Context
		errorChannel          chan error
		errorOutput           []byte
		pipelineId            uuid.UUID
		cacheService          cache.Cache
		stopReadLogsChannel   chan bool
		finishReadLogsChannel chan bool
	}
	tests := []struct {
		name    string
		mocks   func()
		args    args
		wantErr bool
	}{
		{
			name: "Error during HSet operation",
			mocks: func() {
				mock.ExpectHSet(pipelineId.String(), subKey).SetErr(fmt.Errorf(errorMessage))
			},
			args: args{
				ctx:                   context.Background(),
				errorChannel:          errorChannel,
				errorOutput:           nil,
				pipelineId:            pipelineId,
				cacheService:          &redis.Cache{Client: client},
				stopReadLogsChannel:   nil,
				finishReadLogsChannel: nil,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mocks()
			if err := processRunError(tt.args.errorChannel, tt.args.errorOutput, tt.args.pipelineId, tt.args.cacheService, tt.args.stopReadLogsChannel, tt.args.finishReadLogsChannel); (err != nil) != tt.wantErr {
				t.Errorf("processRunError() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_processCompileSuccess(t *testing.T) {
	client, mock := redismock.NewClientMock()
	pipelineId := uuid.New()
	output := "output"
	cacheMock := &redis.Cache{Client: client}
	marshalLogs, _ := json.Marshal(cache.Logs)
	marshalCompileOutput, _ := json.Marshal(cache.CompileOutput)
	marshalRunOutput, _ := json.Marshal(cache.RunOutput)
	marshalRunError, _ := json.Marshal(cache.RunError)
	outputMarshal, _ := json.Marshal(output)
	marshalEmptyString, _ := json.Marshal("")
	type args struct {
		ctx          context.Context
		output       []byte
		pipelineId   uuid.UUID
		cacheService cache.Cache
	}
	tests := []struct {
		name    string
		mocks   func()
		args    args
		wantErr bool
	}{
		{
			name: "Error during set value to CompileOutput subKey",
			mocks: func() {
			},
			args: args{
				ctx:          context.Background(),
				output:       []byte(output),
				pipelineId:   pipelineId,
				cacheService: cacheMock,
			},
			wantErr: true,
		},
		{
			name: "Error during set value to RunOutput subKey",
			mocks: func() {
				mock.ExpectHSet(pipelineId.String(), marshalCompileOutput, outputMarshal).SetVal(1)
			},
			args: args{
				ctx:          context.Background(),
				output:       []byte(output),
				pipelineId:   pipelineId,
				cacheService: cacheMock,
			},
			wantErr: true,
		},
		{
			name: "Error during set value to RunError subKey",
			mocks: func() {
				mock.ExpectHSet(pipelineId.String(), marshalCompileOutput, outputMarshal).SetVal(1)
				mock.ExpectHSet(pipelineId.String(), marshalRunOutput, marshalEmptyString).SetVal(1)
			},
			args: args{
				ctx:          context.Background(),
				output:       []byte(output),
				pipelineId:   pipelineId,
				cacheService: cacheMock,
			},
			wantErr: true,
		},
		{
			name: "Error during set value to Logs subKey",
			mocks: func() {
				mock.ExpectHSet(pipelineId.String(), marshalCompileOutput, outputMarshal).SetVal(1)
				mock.ExpectHSet(pipelineId.String(), marshalRunOutput, marshalEmptyString).SetVal(1)
				mock.ExpectHSet(pipelineId.String(), marshalRunError, marshalEmptyString).SetVal(1)
			},
			args: args{
				ctx:          context.Background(),
				output:       []byte(output),
				pipelineId:   pipelineId,
				cacheService: cacheMock,
			},
			wantErr: true,
		},
		{
			name: "Error during set value to Graph subKey",
			mocks: func() {
				mock.ExpectHSet(pipelineId.String(), marshalCompileOutput, outputMarshal).SetVal(1)
				mock.ExpectHSet(pipelineId.String(), marshalRunOutput, marshalEmptyString).SetVal(1)
				mock.ExpectHSet(pipelineId.String(), marshalRunError, marshalEmptyString).SetVal(1)
				mock.ExpectHSet(pipelineId.String(), marshalLogs, marshalEmptyString).SetVal(1)
			},
			args: args{
				ctx:          context.Background(),
				output:       []byte(output),
				pipelineId:   pipelineId,
				cacheService: cacheMock,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mocks()
			if err := processCompileSuccess(tt.args.output, tt.args.pipelineId, tt.args.cacheService); (err != nil) != tt.wantErr {
				t.Errorf("processCompileSuccess() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_readGraphFile(t *testing.T) {
	pipelineLifeCycleCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	type args struct {
		pipelineLifeCycleCtx context.Context
		backgroundCtx        context.Context
		cacheService         cache.Cache
		graphFilePath        string
		pipelineId           uuid.UUID
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Successfully saving the prepared graph to the cache",
			args: args{
				pipelineLifeCycleCtx: pipelineLifeCycleCtx,
				backgroundCtx:        context.Background(),
				cacheService:         cacheService,
				graphFilePath:        graphFilePath,
				pipelineId:           uuid.New(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readGraphFile(tt.args.pipelineLifeCycleCtx, tt.args.cacheService, tt.args.graphFilePath, tt.args.pipelineId)
			if v, _ := cacheService.GetValue(tt.args.backgroundCtx, tt.args.pipelineId, cache.Graph); v == nil {
				t.Errorf("readGraphFile() error: the graph was not cached")
			}
		})
	}
}

func Test_cancelCheck(t *testing.T) {
	type args struct {
		timeout        time.Duration
		cancelWaitTime time.Duration
		setCancel      bool
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Successfully canceling the pipeline immediately",
			args: args{
				timeout:        5 * time.Minute,
				cancelWaitTime: 0,
				setCancel:      true,
			},
		},
		{
			name: "Successfully canceling the pipeline after timeout",
			args: args{
				timeout:        2 * pauseDuration,
				cancelWaitTime: 3 * pauseDuration,
				setCancel:      true,
			},
		},
		{
			name: "Successfully canceling the pipeline after timeout (immediate timeout)",
			args: args{
				timeout:        0,
				cancelWaitTime: 3 * pauseDuration,
				setCancel:      true,
			},
		},
		{
			name: "Successfully timing out the pipeline without cancel",
			args: args{
				timeout:        3 * pauseDuration,
				cancelWaitTime: 0,
				setCancel:      false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), tt.args.timeout)
			defer cancel()

			id := uuid.New()
			localCache := local.New(ctx)

			go cancelCheck(ctx, id, cancel, localCache)

			if tt.args.setCancel {
				_ = localCache.SetValue(ctx, id, cache.Canceled, true)
			}

			// Wait some time for the cancelCheck to be executed
			time.Sleep(5 * pauseDuration)

			if err := ctx.Err(); err == nil {
				t.Errorf("cancelCheck() error expected, err = %v", err)
			}
		})
	}
}
