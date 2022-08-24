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
	"context"
	"fmt"
	"io/fs"
	"log"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/cache"
	"beam.apache.org/playground/backend/internal/cache/local"
	"beam.apache.org/playground/backend/internal/components"
	"beam.apache.org/playground/backend/internal/constants"
	"beam.apache.org/playground/backend/internal/db"
	datastoreDb "beam.apache.org/playground/backend/internal/db/datastore"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/db/mapper"
	"beam.apache.org/playground/backend/internal/db/schema"
	"beam.apache.org/playground/backend/internal/db/schema/migration"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/tests/test_cleaner"
	"beam.apache.org/playground/backend/internal/tests/test_data"
	"beam.apache.org/playground/backend/internal/tests/test_utils"
	"beam.apache.org/playground/backend/internal/utils"
)

const (
	bufSize               = 1024 * 1024
	javaConfig            = "{\n  \"compile_cmd\": \"javac\",\n  \"run_cmd\": \"java\",\n  \"test_cmd\": \"java\",\n  \"compile_args\": [\n    \"-d\",\n    \"bin\",\n    \"-classpath\"\n  ],\n  \"run_args\": [\n    \"-cp\",\n    \"bin:\"\n  ],\n  \"test_args\": [\n    \"-cp\",\n    \"bin:\",\n    \"JUnit\"\n  ]\n}"
	javaLogConfigFilename = "logging.properties"
	baseFileFolder        = "executable_files"
	configFolder          = "configs"
)

var lis *bufconn.Listener
var cacheService cache.Cache
var dbClient db.Database
var opt goleak.Option
var ctx context.Context

func TestMain(m *testing.M) {
	server := setup()
	opt = goleak.IgnoreCurrent()
	exitValue := m.Run()
	teardown(server)
	os.Exit(exitValue)
}

func setup() *grpc.Server {
	ctx = context.Background()
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
	cacheService = local.New(ctx)

	// setup database
	datastoreEmulatorHost := os.Getenv(constants.EmulatorHostKey)
	if datastoreEmulatorHost == "" {
		if err = os.Setenv(constants.EmulatorHostKey, constants.EmulatorHostValue); err != nil {
			panic(err)
		}
	}
	dbClient, err = datastoreDb.New(ctx, mapper.NewPrecompiledObjectMapper(), constants.EmulatorProjectId)
	if err != nil {
		panic(err)
	}

	// setup environment variables
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
	if err = os.Setenv("SDK_CONFIG", "../../../sdks-emulator.yaml"); err != nil {
		panic(err)
	}
	if err = os.Setenv("PROPERTY_PATH", "../../."); err != nil {
		panic(err)
	}
	if err = os.Setenv(constants.DatastoreNamespaceKey, "main"); err != nil {
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

	// setup app props
	props, err := environment.NewProperties(appEnv.PropertyPath())
	if err != nil {
		panic(err)
	}

	// setup initial data
	versions := []schema.Version{
		new(migration.InitialStructure),
	}
	dbSchema := schema.New(ctx, dbClient, appEnv, props, versions)
	actualSchemaVersion, err := dbSchema.InitiateData()
	if err != nil {
		panic(err)
	}
	appEnv.SetSchemaVersion(actualSchemaVersion)

	// download test data to the Datastore Emulator
	test_data.DownloadCatalogsWithMockData(ctx)

	cacheComponent := components.NewService(cacheService, dbClient)
	entityMapper := mapper.NewDatastoreMapper(ctx, appEnv, props)

	pb.RegisterPlaygroundServiceServer(s, &playgroundController{
		env:            environment.NewEnvironment(*networkEnv, *sdkEnv, *appEnv),
		cacheService:   cacheService,
		db:             dbClient,
		props:          props,
		entityMapper:   entityMapper,
		cacheComponent: cacheComponent,
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

	test_data.RemoveCatalogsWithMockData(ctx)
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
			client, closeFunc := getPlaygroundServiceClient(tt.args.ctx, t)
			defer closeFunc()
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
					_, err = cacheService.GetValue(tt.args.ctx, uuid.MustParse(response.PipelineUuid), cache.Status)
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
	pipelineId := uuid.New()
	wantStatus := pb.Status_STATUS_FINISHED
	client, closeFunc := getPlaygroundServiceClient(ctx, t)
	defer closeFunc()

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
	pipelineId := uuid.New()
	compileOutput := "MOCK_COMPILE_OUTPUT"
	client, closeFunc := getPlaygroundServiceClient(ctx, t)
	defer closeFunc()

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
	pipelineId := uuid.New()
	runOutput := "MOCK_RUN_OUTPUT"
	client, closeFunc := getPlaygroundServiceClient(ctx, t)
	defer closeFunc()

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
	pipelineId := uuid.New()
	logs := "MOCK_LOGS"
	client, closeFunc := getPlaygroundServiceClient(ctx, t)
	defer closeFunc()

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
	pipelineId := uuid.New()
	runError := "MOCK_RUN_ERROR"
	client, closeFunc := getPlaygroundServiceClient(ctx, t)
	defer closeFunc()

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
	pipelineId := uuid.New()
	client, closeFunc := getPlaygroundServiceClient(ctx, t)
	defer closeFunc()

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

func TestPlaygroundController_SaveSnippet(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	client, closeFunc := getPlaygroundServiceClient(ctx, t)
	defer closeFunc()

	type args struct {
		ctx  context.Context
		info *pb.SaveSnippetRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// Test case with calling SaveSnippet method with incorrect sdk.
		// As a result, want to receive an error.
		{
			name: "SaveSnippet with incorrect sdk",
			args: args{
				ctx: ctx,
				info: &pb.SaveSnippetRequest{
					Files: []*pb.SnippetFile{{Name: "MOCK_NAME", Content: "MOCK_CONTENT"}},
					Sdk:   pb.Sdk_SDK_UNSPECIFIED,
				},
			},
			wantErr: true,
		},
		// Test case with calling SaveSnippet method with empty content.
		// As a result, want to receive an error.
		{
			name: "SaveSnippet with empty content",
			args: args{
				ctx: ctx,
				info: &pb.SaveSnippetRequest{
					Files: []*pb.SnippetFile{{Name: "MOCK_NAME", Content: ""}},
					Sdk:   pb.Sdk_SDK_JAVA,
				},
			},
			wantErr: true,
		},
		// Test case with calling SaveSnippet method with a simple entity.
		// As a result, want to receive a generated ID.
		{
			name: "SaveSnippet with a simple entity",
			args: args{
				ctx: ctx,
				info: &pb.SaveSnippetRequest{
					Files: []*pb.SnippetFile{{Name: "MOCK_NAME", Content: "MOCK_CONTENT"}},
					Sdk:   pb.Sdk_SDK_GO,
				},
			},
			wantErr: false,
		},
		// Test case with calling SaveSnippet method with too large entity.
		// As a result, want to receive an error.
		{
			name: "SaveSnippet with too large entity",
			args: args{
				ctx: ctx,
				info: &pb.SaveSnippetRequest{
					Files: []*pb.SnippetFile{{Name: "MOCK_NAME", Content: test_utils.RandomString(1000001)}},
					Sdk:   pb.Sdk_SDK_JAVA,
				},
			},
			wantErr: true,
		},
		// Test case with calling SaveSnippet method with files are nil.
		// As a result, want to receive an error.
		{
			name: "SaveSnippet with files are nil",
			args: args{
				ctx: ctx,
				info: &pb.SaveSnippetRequest{
					Files: nil,
					Sdk:   pb.Sdk_SDK_JAVA,
				},
			},
			wantErr: true,
		},
		// Test case with calling SaveSnippet method with files are empty array.
		// As a result, want to receive an error.
		{
			name: "SaveSnippet with files are empty array",
			args: args{
				ctx: ctx,
				info: &pb.SaveSnippetRequest{
					Files: []*pb.SnippetFile{},
					Sdk:   pb.Sdk_SDK_JAVA,
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.SaveSnippet(ctx, tt.args.info)
			if (err != nil) != tt.wantErr {
				t.Errorf("PlaygroundController_SaveSnippet() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if len(got.Id) != 11 {
					t.Errorf("PlaygroundController_SaveSnippet() unexpected generated ID")
				}
				test_cleaner.CleanFiles(ctx, t, got.Id, 1)
				test_cleaner.CleanSnippet(ctx, t, got.Id)
			}
		})
	}
}

func TestPlaygroundController_GetSnippet(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	client, closeFunc := getPlaygroundServiceClient(ctx, t)
	defer closeFunc()
	nowDate := time.Now()

	type args struct {
		ctx  context.Context
		info *pb.GetSnippetRequest
	}
	tests := []struct {
		name      string
		args      args
		prepare   func()
		wantErr   bool
		cleanData func()
	}{
		// Test case with calling GetSnippet method with ID that is not in the database.
		// As a result, want to receive an error.
		{
			name: "GetSnippet when the entity not found",
			args: args{
				ctx:  ctx,
				info: &pb.GetSnippetRequest{Id: "MOCK_ID_G"},
			},
			prepare:   func() {},
			wantErr:   true,
			cleanData: func() {},
		},
		// Test case with calling GetSnippet method with a correct ID.
		// As a result, want to receive a snippet entity.
		{
			name: "GetSnippet with correct ID",
			args: args{
				ctx:  ctx,
				info: &pb.GetSnippetRequest{Id: "MOCK_ID"},
			},
			prepare: func() {
				_ = dbClient.PutSnippet(ctx, "MOCK_ID",
					&entity.Snippet{
						Snippet: &entity.SnippetEntity{
							Sdk:           utils.GetSdkKey(ctx, pb.Sdk_SDK_JAVA.String()),
							PipeOpts:      "MOCK_OPTIONS",
							Created:       nowDate,
							Origin:        constants.UserSnippetOrigin,
							NumberOfFiles: 1,
						},
						Files: []*entity.FileEntity{{
							Content: "MOCK_CONTENT",
							IsMain:  false,
						}},
					},
				)
			},
			wantErr: false,
			cleanData: func() {
				test_cleaner.CleanFiles(ctx, t, "MOCK_ID", 1)
				test_cleaner.CleanSnippet(ctx, t, "MOCK_ID")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			got, err := client.GetSnippet(tt.args.ctx, tt.args.info)
			if (err != nil) != tt.wantErr {
				t.Errorf("PlaygroundController_GetSnippet() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if got.Files[0].Content != "MOCK_CONTENT" || got.Sdk != 1 || got.PipelineOptions != "MOCK_OPTIONS" {
					t.Errorf("PlaygroundController_GetSnippet() unexpected response")
				}
			}
			tt.cleanData()
		})
	}
}

func TestPlaygroundController_GetPrecompiledObjects(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	client, closeFunc := getPlaygroundServiceClient(ctx, t)
	defer closeFunc()

	type args struct {
		ctx  context.Context
		info *pb.GetPrecompiledObjectsRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		wantSdk pb.Sdk
	}{
		{
			name: "Getting the example catalog when the category is empty and SDK is Java",
			args: args{
				ctx:  ctx,
				info: &pb.GetPrecompiledObjectsRequest{Sdk: pb.Sdk_SDK_JAVA, Category: ""},
			},
			wantSdk: pb.Sdk_SDK_JAVA,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.GetPrecompiledObjects(ctx, tt.args.info)
			if (err != nil) != tt.wantErr {
				t.Errorf("PlaygroundController_GetPrecompiledObjects() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got.SdkCategories) == 0 ||
				got.SdkCategories[0].Sdk != tt.wantSdk ||
				len(got.SdkCategories[0].Categories) == 0 {
				t.Error("PlaygroundController_GetPrecompiledObjects() unexpected result")
			}
		})
	}
}

func TestPlaygroundController_GetPrecompiledObject(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	client, closeFunc := getPlaygroundServiceClient(ctx, t)
	defer closeFunc()

	type args struct {
		ctx  context.Context
		info *pb.GetPrecompiledObjectRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Getting an example in the usual case",
			args: args{
				ctx:  ctx,
				info: &pb.GetPrecompiledObjectRequest{CloudPath: "SDK_JAVA/PRECOMPILED_OBJECT_TYPE_EXAMPLE/MOCK_DEFAULT_EXAMPLE"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.GetPrecompiledObject(ctx, tt.args.info)
			if (err != nil) != tt.wantErr {
				t.Errorf("PlaygroundController_GetPrecompiledObject() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.PrecompiledObject.Multifile != false ||
				got.PrecompiledObject.CloudPath != "SDK_JAVA/PRECOMPILED_OBJECT_TYPE_EXAMPLE/MOCK_DEFAULT_EXAMPLE" ||
				got.PrecompiledObject.Name != "MOCK_DEFAULT_EXAMPLE" ||
				got.PrecompiledObject.Type != pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE ||
				got.PrecompiledObject.ContextLine != 10 ||
				got.PrecompiledObject.PipelineOptions != "MOCK_P_OPTS" ||
				got.PrecompiledObject.Link != "MOCK_PATH" ||
				got.PrecompiledObject.Description != "MOCK_DESCR" ||
				!got.PrecompiledObject.DefaultExample {
				t.Error("PlaygroundController_GetPrecompiledObject() unexpected result")
			}
		})
	}
}

func TestPlaygroundController_GetPrecompiledObjectCode(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	client, closeFunc := getPlaygroundServiceClient(ctx, t)
	defer closeFunc()

	type args struct {
		ctx  context.Context
		info *pb.GetPrecompiledObjectCodeRequest
	}
	tests := []struct {
		name         string
		args         args
		wantErr      bool
		wantResponse string
	}{
		{
			name: "Getting the code of the specific example in the usual case",
			args: args{
				ctx:  ctx,
				info: &pb.GetPrecompiledObjectCodeRequest{CloudPath: "SDK_JAVA/PRECOMPILED_OBJECT_TYPE_EXAMPLE/MOCK_DEFAULT_EXAMPLE"},
			},
			wantErr:      false,
			wantResponse: "MOCK_CONTENT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.GetPrecompiledObjectCode(ctx, tt.args.info)
			if (err != nil) != tt.wantErr {
				t.Errorf("PlaygroundController_GetPrecompiledObjectCode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.Code != tt.wantResponse {
				t.Errorf("PlaygroundController_GetPrecompiledObjectCode() unexpected result")
			}
		})
	}
}

func TestPlaygroundController_GetPrecompiledObjectOutput(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	client, closeFunc := getPlaygroundServiceClient(ctx, t)
	defer closeFunc()

	type args struct {
		ctx  context.Context
		info *pb.GetPrecompiledObjectOutputRequest
	}
	tests := []struct {
		name         string
		args         args
		wantErr      bool
		wantResponse string
	}{
		{
			name: "Getting the output of the compiled and run example in the usual case",
			args: args{
				ctx:  ctx,
				info: &pb.GetPrecompiledObjectOutputRequest{CloudPath: "SDK_JAVA/PRECOMPILED_OBJECT_TYPE_EXAMPLE/MOCK_DEFAULT_EXAMPLE"},
			},
			wantErr:      false,
			wantResponse: "MOCK_CONTENT_" + constants.PCOutputType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.GetPrecompiledObjectOutput(ctx, tt.args.info)
			if (err != nil) != tt.wantErr {
				t.Errorf("PlaygroundController_GetPrecompiledObjectOutput() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.Output != tt.wantResponse {
				t.Errorf("PlaygroundController_GetPrecompiledObjectOutput() unexpected result")
			}
		})
	}
}

func TestPlaygroundController_GetPrecompiledObjectLogs(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	client, closeFunc := getPlaygroundServiceClient(ctx, t)
	defer closeFunc()

	type args struct {
		ctx  context.Context
		info *pb.GetPrecompiledObjectLogsRequest
	}
	tests := []struct {
		name         string
		args         args
		wantErr      bool
		wantResponse string
	}{
		{
			name: "Getting the logs of the compiled and run example in the usual case",
			args: args{
				ctx:  ctx,
				info: &pb.GetPrecompiledObjectLogsRequest{CloudPath: "SDK_JAVA/PRECOMPILED_OBJECT_TYPE_EXAMPLE/MOCK_DEFAULT_EXAMPLE"},
			},
			wantErr:      false,
			wantResponse: "MOCK_CONTENT_" + constants.PCLogType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.GetPrecompiledObjectLogs(ctx, tt.args.info)
			if (err != nil) != tt.wantErr {
				t.Errorf("PlaygroundController_GetPrecompiledObjectLogs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.Output != tt.wantResponse {
				t.Errorf("PlaygroundController_GetPrecompiledObjectLogs() unexpected result")
			}
		})
	}
}

func TestPlaygroundController_GetPrecompiledObjectGraph(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	client, closeFunc := getPlaygroundServiceClient(ctx, t)
	defer closeFunc()

	type args struct {
		ctx  context.Context
		info *pb.GetPrecompiledObjectGraphRequest
	}
	tests := []struct {
		name         string
		args         args
		wantErr      bool
		wantResponse string
	}{
		{
			name: "Getting the logs of the compiled and run example in the usual case",
			args: args{
				ctx:  ctx,
				info: &pb.GetPrecompiledObjectGraphRequest{CloudPath: "SDK_JAVA/PRECOMPILED_OBJECT_TYPE_EXAMPLE/MOCK_DEFAULT_EXAMPLE"},
			},
			wantErr:      false,
			wantResponse: "MOCK_CONTENT_" + constants.PCGraphType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.GetPrecompiledObjectGraph(ctx, tt.args.info)
			if (err != nil) != tt.wantErr {
				t.Errorf("PlaygroundController_GetPrecompiledObjectGraph() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.Graph != tt.wantResponse {
				t.Errorf("PlaygroundController_GetPrecompiledObjectGraph() unexpected result")
			}
		})
	}
}

func TestPlaygroundController_GetDefaultPrecompiledObject(t *testing.T) {
	defer goleak.VerifyNone(t, opt)
	client, closeFunc := getPlaygroundServiceClient(ctx, t)
	defer closeFunc()

	type args struct {
		ctx  context.Context
		info *pb.GetDefaultPrecompiledObjectRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Getting a default example in the usual case",
			args: args{
				ctx:  ctx,
				info: &pb.GetDefaultPrecompiledObjectRequest{Sdk: pb.Sdk_SDK_JAVA},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.GetDefaultPrecompiledObject(ctx, tt.args.info)
			if (err != nil) != tt.wantErr {
				t.Errorf("PlaygroundController_GetDefaultPrecompiledObject() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.PrecompiledObject.Multifile != false ||
				got.PrecompiledObject.CloudPath != "SDK_JAVA/PRECOMPILED_OBJECT_TYPE_EXAMPLE/MOCK_DEFAULT_EXAMPLE" ||
				got.PrecompiledObject.Name != "MOCK_DEFAULT_EXAMPLE" ||
				got.PrecompiledObject.Type != pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE ||
				got.PrecompiledObject.ContextLine != 10 ||
				got.PrecompiledObject.PipelineOptions != "MOCK_P_OPTS" ||
				got.PrecompiledObject.Link != "MOCK_PATH" ||
				got.PrecompiledObject.Description != "MOCK_DESCR" ||
				!got.PrecompiledObject.DefaultExample {
				t.Error("PlaygroundController_GetDefaultPrecompiledObject() unexpected result")
			}
		})
	}
}

func getPlaygroundServiceClient(ctx context.Context, t *testing.T) (pb.PlaygroundServiceClient, func()) {
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	closeFunc := func() {
		err = conn.Close()
		if err != nil {
			t.Fatalf("Failed to close grpc connection: %v", err)
		}
	}
	client := pb.NewPlaygroundServiceClient(conn)
	return client, closeFunc
}
