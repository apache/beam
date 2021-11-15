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
package main

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
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"io/fs"
	"log"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	bufSize    = 1024 * 1024
	javaConfig = "{\n  \"compile_cmd\": \"javac\",\n  \"run_cmd\": \"java\",\n  \"compile_args\": [\"-d\", \"bin\", \"-classpath\"],\n  \"run_args\": [\"-cp\", \"bin:\"]\n}"
)

var lis *bufconn.Listener
var cacheService cache.Cache
var opt goleak.Option

func TestMain(m *testing.M) {
	server := setup()
	opt = goleak.IgnoreCurrent()
	exitValue := m.Run()
	teardown(server)
	os.Exit(exitValue)
}

func setup() *grpc.Server {
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()

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

	// setup cache
	cacheService = local.New(context.Background())

	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	os.Setenv("BEAM_SDK", pb.Sdk_SDK_JAVA.String())
	os.Setenv("APP_WORK_DIR", path)

	networkEnv, err := environment.GetNetworkEnvsFromOsEnvs()
	if err != nil {
		panic(err)
	}
	appEnv, err := environment.GetApplicationEnvsFromOsEnvs()
	if err != nil {
		panic(err)
	}
	sdkEnv, err := environment.ConfigureBeamEnvs(appEnv.WorkingDir())
	if err != nil {
		panic(err)
	}
	pb.RegisterPlaygroundServiceServer(s, &playgroundController{
		env:          environment.NewEnvironment(*networkEnv, *sdkEnv, *appEnv),
		cacheService: cacheService,
	})
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()
	return s
}

func teardown(server *grpc.Server) {
	server.Stop()

	err := os.RemoveAll("configs")
	if err != nil {
		panic(fmt.Errorf("error during test setup: %s", err.Error()))
	}
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func TestPlaygroundController_RunCode(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	type args struct {
		ctx     context.Context
		request *pb.RunCodeRequest
	}
	tests := []struct {
		name       string
		args       args
		wantStatus pb.Status
		wantErr    bool
	}{
		{
			// Test case with calling RunCode method with incorrect SDK.
			// As a result want to receive error.
			name: "RunCode with incorrect sdk",
			args: args{
				ctx: context.Background(),
				request: &pb.RunCodeRequest{
					Code: "MOCK_CODE",
					Sdk:  pb.Sdk_SDK_UNSPECIFIED,
				},
			},
			wantErr: true,
		},
		{
			// Test case with calling RunCode method with correct SDK.
			// As a result want to receive response with pipelineId and status into cache should be set as Status_STATUS_COMPILING.
			name: "RunCode with correct sdk",
			args: args{
				ctx: context.Background(),
				request: &pb.RunCodeRequest{
					Code: "MOCK_CODE",
					Sdk:  pb.Sdk_SDK_JAVA,
				},
			},
			wantStatus: pb.Status_STATUS_COMPILING,
			wantErr:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn, err := grpc.DialContext(tt.args.ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
			if err != nil {
				t.Fatalf("Failed to dial bufnet: %v", err)
			}
			defer conn.Close()
			client := pb.NewPlaygroundServiceClient(conn)
			response, err := client.RunCode(tt.args.ctx, tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("PlaygroundController_RunCode() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if response == nil {
					t.Errorf("PlaygroundController_RunCode() response shoudn't be nil")
				} else {
					if response.PipelineUuid == "" {
						t.Errorf("PlaygroundController_RunCode() response.pipeLineId shoudn't be nil")
					} else {
						path := os.Getenv("APP_WORK_DIR") + "/executable_files"
						os.RemoveAll(path)
					}
					status, _ := cacheService.GetValue(tt.args.ctx, uuid.MustParse(response.PipelineUuid), cache.Status)
					if status == nil {
						t.Errorf("PlaygroundController_RunCode() status shoudn't be nil")
					}
					if !reflect.DeepEqual(status, tt.wantStatus) {
						t.Errorf("PlaygroundController_RunCode() status = %v, wantStatus %v", status, tt.wantStatus)
					}

				}
			}
		})
	}
}

func TestPlaygroundController_CheckStatus(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	ctx := context.Background()
	pipelineId := uuid.New()
	wantStatus := pb.Status_STATUS_FINISHED
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := pb.NewPlaygroundServiceClient(conn)

	type args struct {
		ctx     context.Context
		request *pb.CheckStatusRequest
	}
	tests := []struct {
		name       string
		prepare    func()
		args       args
		wantStatus *pb.Status
		wantErr    bool
	}{
		{
			// Test case with calling CheckStatus method with pipelineId which doesn't exist.
			// As a result, want to receive an error.
			name:    "status is not set",
			prepare: func() {},
			args: args{
				ctx:     ctx,
				request: &pb.CheckStatusRequest{PipelineUuid: pipelineId.String()},
			},
			wantStatus: nil,
			wantErr:    true,
		},
		{
			// Test case with calling CheckStatus method with pipelineId which contains status.
			// As a result, want to receive an expected status.
			name: "status exists",
			prepare: func() {
				_ = cacheService.SetValue(ctx, pipelineId, cache.Status, wantStatus)
			},
			args: args{
				ctx:     ctx,
				request: &pb.CheckStatusRequest{PipelineUuid: pipelineId.String()},
			},
			wantStatus: &wantStatus,
			wantErr:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			got, err := client.CheckStatus(ctx, tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("PlaygroundController_CheckStatus() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got == nil && tt.wantStatus != nil {
				t.Errorf("PlaygroundController_CheckStatus() return = %v, want response with status %v", got, tt.wantStatus)
			}
			if got != nil && !reflect.DeepEqual(got.Status, *tt.wantStatus) {
				t.Errorf("PlaygroundController_CheckStatus() return status = %v, want status %v", got.Status, tt.wantStatus)
			}
		})
	}
}

func TestPlaygroundController_GetCompileOutput(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	ctx := context.Background()
	pipelineId := uuid.New()
	compileOutput := "MOCK_COMPILE_OUTPUT"
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := pb.NewPlaygroundServiceClient(conn)

	type args struct {
		ctx  context.Context
		info *pb.GetCompileOutputRequest
	}
	tests := []struct {
		name    string
		prepare func()
		args    args
		want    *pb.GetRunOutputResponse
		wantErr bool
	}{
		{
			name:    "pipelineId doesn't exist",
			prepare: func() {},
			args: args{
				ctx:  ctx,
				info: &pb.GetCompileOutputRequest{PipelineUuid: pipelineId.String()},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "run output exist",
			prepare: func() {
				_ = cacheService.SetValue(ctx, pipelineId, cache.CompileOutput, compileOutput)
			},
			args: args{
				ctx:  ctx,
				info: &pb.GetCompileOutputRequest{PipelineUuid: pipelineId.String()},
			},
			want:    &pb.GetRunOutputResponse{Output: compileOutput},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			got, err := client.GetCompileOutput(tt.args.ctx, tt.args.info)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetCompileOutput() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if !strings.EqualFold(got.Output, tt.want.Output) {
					t.Errorf("GetCompileOutput() got = %v, want %v", got.Output, tt.want.Output)
				}
			}
		})
	}
}

func TestPlaygroundController_GetRunOutput(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	ctx := context.Background()
	pipelineId := uuid.New()
	runOutput := "MOCK_RUN_OUTPUT"
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := pb.NewPlaygroundServiceClient(conn)

	type args struct {
		ctx  context.Context
		info *pb.GetRunOutputRequest
	}
	tests := []struct {
		name    string
		prepare func()
		args    args
		want    *pb.GetRunOutputResponse
		wantErr bool
	}{
		{
			name:    "pipelineId doesn't exist",
			prepare: func() {},
			args: args{
				ctx:  ctx,
				info: &pb.GetRunOutputRequest{PipelineUuid: pipelineId.String()},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "run output exist",
			prepare: func() {
				_ = cacheService.SetValue(ctx, pipelineId, cache.RunOutput, runOutput)
			},
			args: args{
				ctx:  ctx,
				info: &pb.GetRunOutputRequest{PipelineUuid: pipelineId.String()},
			},
			want:    &pb.GetRunOutputResponse{Output: runOutput},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			got, err := client.GetRunOutput(tt.args.ctx, tt.args.info)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetRunOutput() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if !strings.EqualFold(got.Output, tt.want.Output) {
					t.Errorf("GetRunOutput() got = %v, want %v", got.Output, tt.want.Output)
				}
			}
		})
	}
}

func TestPlaygroundController_GetRunError(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	ctx := context.Background()
	pipelineId := uuid.New()
	runError := "MOCK_RUN_ERROR"
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := pb.NewPlaygroundServiceClient(conn)

	type args struct {
		ctx  context.Context
		info *pb.GetRunErrorRequest
	}
	tests := []struct {
		name    string
		prepare func()
		args    args
		want    *pb.GetRunErrorResponse
		wantErr bool
	}{
		{
			name:    "pipelineId doesn't exist",
			prepare: func() {},
			args: args{
				ctx:  ctx,
				info: &pb.GetRunErrorRequest{PipelineUuid: pipelineId.String()},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "run error output doesn't exist",
			prepare: func() {
				_ = cacheService.SetValue(ctx, pipelineId, cache.Status, pb.Status_STATUS_VALIDATING)
			},
			args: args{
				ctx:  ctx,
				info: &pb.GetRunErrorRequest{PipelineUuid: pipelineId.String()},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "run error output exist",
			prepare: func() {
				_ = cacheService.SetValue(ctx, pipelineId, cache.RunError, runError)
			},
			args: args{
				ctx:  ctx,
				info: &pb.GetRunErrorRequest{PipelineUuid: pipelineId.String()},
			},
			want:    &pb.GetRunErrorResponse{Output: runError},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			got, err := client.GetRunError(tt.args.ctx, tt.args.info)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetRunError() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if !strings.EqualFold(got.Output, tt.want.Output) {
					t.Errorf("GetRunError() got = %v, want %v", got.Output, tt.want.Output)
				}
			}
		})
	}
}

func TestPlaygroundController_Cancel(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	ctx := context.Background()
	pipelineId := uuid.New()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := pb.NewPlaygroundServiceClient(conn)

	type args struct {
		ctx  context.Context
		info *pb.CancelRequest
	}
	tests := []struct {
		name      string
		checkFunc func() bool
		args      args
		want      *pb.CancelResponse
		wantErr   bool
	}{
		{
			// Test case with calling Cancel method.
			// As a result, want to find value in cache for cache.Canceled subKey.
			name: "set cancel without error",
			args: args{
				ctx:  ctx,
				info: &pb.CancelRequest{PipelineUuid: pipelineId.String()},
			},
			checkFunc: func() bool {
				value, err := cacheService.GetValue(context.Background(), pipelineId, cache.Canceled)
				if err != nil {
					return false
				}
				return value.(bool)
			},
			want:    &pb.CancelResponse{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := client.Cancel(tt.args.ctx, tt.args.info); (err != nil) != tt.wantErr {
				t.Errorf("Cancel() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.checkFunc() {
				t.Error("Cancel() doesn't set canceled flag")
			}
		})
	}
}

func Test_processCode(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	networkEnvs, err := environment.GetNetworkEnvsFromOsEnvs()
	if err != nil {
		panic(err)
	}
	appEnvs, err := environment.GetApplicationEnvsFromOsEnvs()
	if err != nil {
		panic(err)
	}
	sdkEnv, err := environment.ConfigureBeamEnvs(appEnvs.WorkingDir())
	if err != nil {
		panic(err)
	}

	env := environment.NewEnvironment(*networkEnvs, *sdkEnv, *appEnvs)

	type args struct {
		ctx        context.Context
		env        *environment.Environment
		sdk        pb.Sdk
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
				env:        environment.NewEnvironment(*networkEnvs, *sdkEnv, environment.ApplicationEnvs{}),
				sdk:        pb.Sdk_SDK_JAVA,
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
				env:        env,
				sdk:        pb.Sdk_SDK_JAVA,
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
				env:        env,
				sdk:        pb.Sdk_SDK_JAVA,
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
			expectedRunOutput:     nil,
			expectedRunError:      "error: exit status 1, output: Exception in thread \"main\" java.lang.ArithmeticException: / by zero\n\tat HelloWorld.main(%s.java:3)\n",
			args: args{
				ctx:        context.Background(),
				env:        env,
				sdk:        pb.Sdk_SDK_JAVA,
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
			expectedRunOutput:     nil,
			expectedRunError:      nil,
			args: args{
				ctx:        context.Background(),
				env:        env,
				sdk:        pb.Sdk_SDK_JAVA,
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
				env:        env,
				sdk:        pb.Sdk_SDK_JAVA,
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
					err := cacheService.SetValue(ctx, pipelineId, cache.Canceled, true)
					if err != nil {
						panic(err)
					}
				}(tt.args.ctx, tt.args.pipelineId)
			}
			processCode(tt.args.ctx, cacheService, lc, exec, tt.args.pipelineId, tt.args.env, tt.args.sdk)

			status, _ := cacheService.GetValue(tt.args.ctx, tt.args.pipelineId, cache.Status)
			if !reflect.DeepEqual(status, tt.expectedStatus) {
				t.Errorf("processCode() set status: %s, but expectes: %s", status, tt.expectedStatus)
			}

			compileOutput, _ := cacheService.GetValue(tt.args.ctx, tt.args.pipelineId, cache.CompileOutput)
			if tt.expectedCompileOutput != "" && tt.expectedCompileOutput != nil && strings.Contains(tt.expectedCompileOutput.(string), "%s") {
				tt.expectedCompileOutput = fmt.Sprintf(tt.expectedCompileOutput.(string), lc.GetAbsoluteExecutableFilePath())
			}
			if !reflect.DeepEqual(compileOutput, tt.expectedCompileOutput) {
				t.Errorf("processCode() set compileOutput: %s, but expectes: %s", compileOutput, tt.expectedCompileOutput)
			}

			runOutput, _ := cacheService.GetValue(tt.args.ctx, tt.args.pipelineId, cache.RunOutput)
			if tt.expectedRunOutput != "" && tt.expectedRunOutput != nil && strings.Contains(tt.expectedRunOutput.(string), "%s") {
				tt.expectedRunOutput = fmt.Sprintf(tt.expectedRunOutput.(string), tt.args.pipelineId)
			}
			if !reflect.DeepEqual(runOutput, tt.expectedRunOutput) {
				t.Errorf("processCode() set runOutput: %s, but expectes: %s", runOutput, tt.expectedRunOutput)
			}

			runError, _ := cacheService.GetValue(tt.args.ctx, tt.args.pipelineId, cache.RunError)
			if tt.expectedRunError != "" && tt.expectedRunError != nil && strings.Contains(tt.expectedRunError.(string), "%s") {
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
