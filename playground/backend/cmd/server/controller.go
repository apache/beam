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
	pb "beam.apache.org/playground/backend/internal/api"
	"beam.apache.org/playground/backend/internal/errors"
	"beam.apache.org/playground/backend/internal/executors"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"context"
	"fmt"
	"github.com/google/uuid"
	"google.golang.org/grpc/grpclog"
)

type playgroundController struct {
	pb.UnimplementedPlaygroundServiceServer
}

// TODO change to using cache
var results = make(map[string]interface{})

//RunCode is running code from requests using a particular SDK
func (controller *playgroundController) RunCode(ctx context.Context, info *pb.RunCodeRequest) (*pb.RunCodeResponse, error) {
	pipeLineId := uuid.New()

	// create file system service
	lc, err := fs_tool.NewLifeCycle(info.Sdk, pipeLineId)
	if err != nil {
		grpclog.Error("RunCode: NewLifeCycle:" + err.Error())
		return nil, errors.InternalError("Run code", "Error during creating file system service: "+err.Error())
	}

	// create folders
	err = lc.CreateFolders()
	if err != nil {
		grpclog.Error("RunCode: CreateFolders:" + err.Error())
		return nil, errors.InternalError("Run code", "Error during preparing folders: "+err.Error())
	}

	// create file with code
	_, err = lc.CreateExecutableFile(info.Code)
	if err != nil {
		grpclog.Error("RunCode: CreateExecutableFile:" + err.Error())
		return nil, errors.InternalError("Run code", "Error during creating file with code: "+err.Error())
	}

	// create executor
	exec, err := executors.NewExecutor(info.Sdk, lc)
	if err != nil {
		grpclog.Error("RunCode: NewExecutor:" + err.Error())
		return nil, errors.InternalError("Run code", "Error during creating executor: "+err.Error())
	}

	go runCode(lc, exec, pipeLineId)

	pipelineInfo := pb.RunCodeResponse{PipelineUuid: pipeLineId.String()}
	return &pipelineInfo, nil
}

//CheckStatus is checking status for the specific pipeline by PipelineUuid
func (controller *playgroundController) CheckStatus(ctx context.Context, info *pb.CheckStatusRequest) (*pb.CheckStatusResponse, error) {
	// TODO implement this method
	status := pb.CheckStatusResponse{Status: pb.Status_STATUS_FINISHED}
	return &status, nil
}

//GetRunOutput is returning output of execution for specific pipeline by PipelineUuid
func (controller *playgroundController) GetRunOutput(ctx context.Context, info *pb.GetRunOutputRequest) (*pb.GetRunOutputResponse, error) {
	// TODO implement this method
	pipelineResult := pb.GetRunOutputResponse{Output: "Test Pipeline Result"}

	return &pipelineResult, nil
}

//GetCompileOutput is returning output of compilation for specific pipeline by PipelineUuid
func (controller *playgroundController) GetCompileOutput(ctx context.Context, info *pb.GetCompileOutputRequest) (*pb.GetCompileOutputResponse, error) {
	// TODO implement this method
	compileOutput := pb.GetCompileOutputResponse{Output: "test compile output"}
	return &compileOutput, nil
}

// runCode validates, compiles and runs code by pipelineId.
// During each operation updates status of execution and saves it into cache.
// In case of compilation failed saves logs to cache.
// After success code running saves output to cache.
func runCode(lc *fs_tool.LifeCycle, exec *executors.Executor, pipelineId uuid.UUID) {
	// validate
	err := exec.Validate()
	if err != nil {
		// error during validation
		grpclog.Error("RunCode: Validate" + err.Error())

		// set to cache pipeLineId: cache.Tag_StatusTag: pb.Status_STATUS_ERROR
		key := fmt.Sprintf("%s:%s", pipelineId, "StatusTag")
		results[key] = pb.Status_STATUS_ERROR
	}

	err = exec.Compile()
	if err != nil {
		// error during compilation
		grpclog.Error("RunCode: Compile" + err.Error())

		// set to cache pipeLineId: cache.Tag_StatusTag: pb.Status_STATUS_ERROR
		key := fmt.Sprintf("%s:%s", pipelineId, "StatusTag")
		results[key] = pb.Status_STATUS_ERROR

		// set to cache pipeLineId: cache.Tag_CompileOutputTag: err.Error()
		key = fmt.Sprintf("%s:%s", pipelineId, "CompileOutputTag")
		results[key] = err.Error()
	} else {
		// compilation success
		// set to cache pipeLineId: cache.Tag_StatusTag: pb.Status_STATUS_EXECUTING
		key := fmt.Sprintf("%s:%s", pipelineId, "StatusTag")
		results[key] = pb.Status_STATUS_EXECUTING

		output, err := exec.Run("HelloWorld")
		if err != nil {
			// error during run code
			grpclog.Error("RunCode: Run" + err.Error())

			// set to cache pipeLineId: cache.Tag_RunOutputTag: err.Error()
			key := fmt.Sprintf("%s:%s", pipelineId, "RunOutputTag")
			results[key] = err.Error()

			// set to cache pipeLineId: cache.Tag_StatusTag: pb.Status_STATUS_ERROR
			key = fmt.Sprintf("%s:%s", pipelineId, "StatusTag")
			results[key] = pb.Status_STATUS_ERROR
		} else {
			// run code success
			// set to cache pipeLineId: cache.Tag_RunOutputTag: output
			key := fmt.Sprintf("%s:%s", pipelineId, "RunOutputTag")
			results[key] = output

			// set to cache pipeLineId: cache.Tag_StatusTag: pb.Status_STATUS_FINISHED
			key = fmt.Sprintf("%s:%s", pipelineId, "StatusTag")
			results[key] = pb.Status_STATUS_FINISHED
		}
	}
	err = lc.DeleteCompiledFile()
	if err != nil {
		grpclog.Error("RunCode: DeleteCompiledFile" + err.Error())
	}
	err = lc.DeleteExecutableFile()
	if err != nil {
		grpclog.Error("RunCode: DeleteExecutableFile" + err.Error())
	}
	err = lc.DeleteFolders()
	if err != nil {
		grpclog.Error("RunCode: DeleteFolders" + err.Error())
	}
}
