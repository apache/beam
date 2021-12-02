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
	bufSize               = 1024 * 1024
	javaConfig            = "{\n  \"compile_cmd\": \"javac\",\n  \"run_cmd\": \"java\",\n  \"compile_args\": [\"-d\", \"bin\", \"-classpath\"],\n  \"run_args\": [\"-cp\", \"bin:\"]\n}"
	javaLogConfigFilename = "logging.properties"
	baseFileFolder        = "executable_files"
	configFolder          = "configs"
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
	err := os.MkdirAll(configFolder, fs.ModePerm)
	if err != nil {
		panic(err)
	}
	filePath := filepath.Join(configFolder, pb.Sdk_SDK_JAVA.String()+".json")
	err = os.WriteFile(filePath, []byte(javaConfig), 0600)
	if err != nil {
		panic(err)
	}

	// create log config file
	_, err = os.Create(javaLogConfigFilename)
	if err != nil {
		panic(err)
	}

	// setup cache
	cacheService = local.New(context.Background())

	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	if err = os.Setenv("BEAM_SDK", pb.Sdk_SDK_JAVA.String()); err != nil {
		panic(err)
	}
	if err = os.Setenv("APP_WORK_DIR", path); err != nil {
		panic(err)
	}

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

	removeDir(configFolder)
	removeDir(javaLogConfigFilename)
	removeDir(baseFileFolder)
}

func removeDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		panic(fmt.Errorf("error during remove dir %s: %s", dir, err.Error()))
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
		name    string
		args    args
		wantErr bool
	}{
		{
			// Test case with calling RunCode method with incorrect SDK.
			// As a result, want to receive an error.
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
			// As a result, want to receive response with pipelineId and status into cache should be set as Status_STATUS_COMPILING.
			name: "RunCode with correct sdk",
			args: args{
				ctx: context.Background(),
				request: &pb.RunCodeRequest{
					Code: "MOCK_CODE",
					Sdk:  pb.Sdk_SDK_JAVA,
				},
			},
			wantErr: false,
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
					// wait for code processing is finished
					time.Sleep(time.Second * 10)
					if response.PipelineUuid == "" {
						t.Errorf("PlaygroundController_RunCode() response.pipeLineId shoudn't be nil")
					}
					_, err := cacheService.GetValue(tt.args.ctx, uuid.MustParse(response.PipelineUuid), cache.Status)
					if err != nil {
						t.Errorf("PlaygroundController_RunCode() status should exist")
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
			// Test case with calling CheckStatus method with incorrect pipelineId.
			// As a result, want to receive an error
			name:    "incorrect pipelineId",
			prepare: func() {},
			args: args{
				ctx:     ctx,
				request: &pb.CheckStatusRequest{PipelineUuid: "NO_UUID_STRING"},
			},
			wantStatus: nil,
			wantErr:    true,
		},
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
		want    *pb.GetCompileOutputResponse
		wantErr bool
	}{
		{
			// Test case with calling GetCompileOutput method with incorrect pipelineId.
			// As a result, want to receive an error
			name:    "incorrect pipelineId",
			prepare: func() {},
			args: args{
				ctx:  ctx,
				info: &pb.GetCompileOutputRequest{PipelineUuid: "NO_UUID_STRING"},
			},
			want:    nil,
			wantErr: true,
		},
		{
			// Test case with calling GetCompileOutput method with pipelineId which doesn't exist.
			// As a result, want to receive an error.
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
			// Test case with calling GetCompileOutput method with pipelineId which contains compile output.
			// As a result, want to receive an expected compile output.
			name: "compile output exist",
			prepare: func() {
				_ = cacheService.SetValue(ctx, pipelineId, cache.CompileOutput, compileOutput)
			},
			args: args{
				ctx:  ctx,
				info: &pb.GetCompileOutputRequest{PipelineUuid: pipelineId.String()},
			},
			want:    &pb.GetCompileOutputResponse{Output: compileOutput},
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
			// Test case with calling GetRunOutput method with incorrect pipelineId.
			// As a result, want to receive an error
			name:    "incorrect pipelineId",
			prepare: func() {},
			args: args{
				ctx:  ctx,
				info: &pb.GetRunOutputRequest{PipelineUuid: "NO_UUID_STRING"},
			},
			want:    nil,
			wantErr: true,
		},
		{
			// Test case with calling GetRunOutput method with pipelineId which doesn't exist.
			// As a result, want to receive an error.
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
			// Test case with calling GetRunOutput method with pipelineId which doesn't contain run output.
			// As a result, want to receive an error.
			name: "run output doesn't exist",
			prepare: func() {
				_ = cacheService.SetValue(ctx, pipelineId, cache.Status, pb.Status_STATUS_EXECUTING)
			},
			args: args{
				ctx:  ctx,
				info: &pb.GetRunOutputRequest{PipelineUuid: pipelineId.String()},
			},
			want:    nil,
			wantErr: true,
		},
		{
			// Test case with calling GetRunOutput method with pipelineId which contains run output.
			// As a result want to receive response with an expected run output.
			name: "run output exist",
			prepare: func() {
				_ = cacheService.SetValue(ctx, pipelineId, cache.RunOutputIndex, 0)
				_ = cacheService.SetValue(ctx, pipelineId, cache.RunOutput, runOutput)
			},
			args: args{
				ctx:  ctx,
				info: &pb.GetRunOutputRequest{PipelineUuid: pipelineId.String()},
			},
			want:    &pb.GetRunOutputResponse{Output: runOutput},
			wantErr: false,
		},
		{
			// Test case with calling RunOutput method with pipelineId which contain run output and index of run output is 1.
			// As a result want to receive response with correct output (output[1:]).
			name: "get the second part",
			prepare: func() {
				_ = cacheService.SetValue(ctx, pipelineId, cache.RunOutputIndex, 1)
				_ = cacheService.SetValue(ctx, pipelineId, cache.RunOutput, runOutput)
			},
			args: args{
				ctx:  ctx,
				info: &pb.GetRunOutputRequest{PipelineUuid: pipelineId.String()},
			},
			want:    &pb.GetRunOutputResponse{Output: runOutput[1:]},
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

func TestPlaygroundController_GetLogs(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	ctx := context.Background()
	pipelineId := uuid.New()
	logs := "MOCK_LOGS"
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := pb.NewPlaygroundServiceClient(conn)

	type args struct {
		ctx  context.Context
		info *pb.GetLogsRequest
	}
	tests := []struct {
		name    string
		prepare func()
		args    args
		want    *pb.GetLogsResponse
		wantErr bool
	}{
		{
			// Test case with calling GetLogs method with incorrect pipelineId.
			// As a result, want to receive an error
			name:    "incorrect pipelineId",
			prepare: func() {},
			args: args{
				ctx:  ctx,
				info: &pb.GetLogsRequest{PipelineUuid: "NO_UUID_STRING"},
			},
			want:    nil,
			wantErr: true,
		},
		{
			// Test case with calling GetRLogs method with pipelineId which doesn't exist.
			// As a result, want to receive an error.
			name:    "pipelineId doesn't exist",
			prepare: func() {},
			args: args{
				ctx:  ctx,
				info: &pb.GetLogsRequest{PipelineUuid: pipelineId.String()},
			},
			want:    nil,
			wantErr: true,
		},
		{
			// Test case with calling GetLogs method with pipelineId which doesn't contain logs.
			// As a result, want to receive an error.
			name: "logs don't exist",
			prepare: func() {
				_ = cacheService.SetValue(ctx, pipelineId, cache.Status, pb.Status_STATUS_EXECUTING)
			},
			args: args{
				ctx:  ctx,
				info: &pb.GetLogsRequest{PipelineUuid: pipelineId.String()},
			},
			want:    nil,
			wantErr: true,
		},
		{
			// Test case with calling GetLogs method with pipelineId which contains logs.
			// As a result want to receive response with an expected logs.
			name: "logs exist",
			prepare: func() {
				_ = cacheService.SetValue(ctx, pipelineId, cache.LogsIndex, 0)
				_ = cacheService.SetValue(ctx, pipelineId, cache.Logs, logs)
			},
			args: args{
				ctx:  ctx,
				info: &pb.GetLogsRequest{PipelineUuid: pipelineId.String()},
			},
			want:    &pb.GetLogsResponse{Output: logs},
			wantErr: false,
		},
		{
			// Test case with calling GetLogs method with pipelineId which contain logs and index of logs is 1.
			// As a result want to receive response with correct logs (logs[1:]).
			name: "get the second part",
			prepare: func() {
				_ = cacheService.SetValue(ctx, pipelineId, cache.LogsIndex, 1)
				_ = cacheService.SetValue(ctx, pipelineId, cache.Logs, logs)
			},
			args: args{
				ctx:  ctx,
				info: &pb.GetLogsRequest{PipelineUuid: pipelineId.String()},
			},
			want:    &pb.GetLogsResponse{Output: logs[1:]},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			got, err := client.GetLogs(tt.args.ctx, tt.args.info)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetLogs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if !strings.EqualFold(got.Output, tt.want.Output) {
					t.Errorf("GetLogs() got = %v, want %v", got.Output, tt.want.Output)
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
			// Test case with calling GetRunError method with incorrect pipelineId.
			// As a result, want to receive an error
			name:    "incorrect pipelineId",
			prepare: func() {},
			args: args{
				ctx:  ctx,
				info: &pb.GetRunErrorRequest{PipelineUuid: "NO_UUID_STRING"},
			},
			want:    nil,
			wantErr: true,
		},
		{
			// Test case with calling GetRunError method with pipelineId which doesn't exist.
			// As a result, want to receive an error.
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
			// Test case with calling GetRunError method with pipelineId which doesn't contain run error.
			// As a result, want to receive an error.
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
			// Test case with calling GetRunError method with pipelineId which contains run error.
			// As a result want to receive response with an expected run error .
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
		args      args
		want      *pb.CancelResponse
		checkFunc func() bool
		wantErr   bool
	}{
		{
			// Test case with calling Cancel method with incorrect pipelineId.
			// As a result, want to receive an error
			name: "incorrect pipelineId",
			args: args{
				ctx:  ctx,
				info: &pb.CancelRequest{PipelineUuid: "NO_UUID_STRING"},
			},
			checkFunc: func() bool {
				return true
			},
			want:    nil,
			wantErr: true,
		},
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
