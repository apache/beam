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
	"beam.apache.org/playground/backend/internal/cloud_bucket"
	"beam.apache.org/playground/backend/internal/code_processing"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/errors"
	"beam.apache.org/playground/backend/internal/logger"
	"beam.apache.org/playground/backend/internal/setup_tools/compile_builder"
	"beam.apache.org/playground/backend/internal/setup_tools/life_cycle"
	"beam.apache.org/playground/backend/internal/utils"
	"context"
	"github.com/google/uuid"
)

// playgroundController processes `gRPC' requests from clients.
// Contains methods to process receiving code, monitor current status of code processing and receive compile/run output.
type playgroundController struct {
	env          *environment.Environment
	cacheService cache.Cache

	pb.UnimplementedPlaygroundServiceServer
}

// RunCode is running code from requests using a particular SDK
// - In case of incorrect sdk returns codes.InvalidArgument
// - In case of error during preparing files/folders returns codes.Internal
// - In case of no errors saves playground.Status_STATUS_EXECUTING as cache.Status into cache and sets expiration time
//   for all cache values which will be saved into cache during processing received code.
//   Returns id of code processing (pipelineId)
func (controller *playgroundController) RunCode(ctx context.Context, info *pb.RunCodeRequest) (*pb.RunCodeResponse, error) {
	// check for correct sdk
	if info.Sdk != controller.env.BeamSdkEnvs.ApacheBeamSdk {
		logger.Errorf("RunCode(): request contains incorrect sdk: %s\n", info.Sdk)
		return nil, errors.InvalidArgumentError("Run code()", "incorrect sdk: "+info.Sdk.String())
	}
	switch info.Sdk {
	case pb.Sdk_SDK_UNSPECIFIED, pb.Sdk_SDK_PYTHON, pb.Sdk_SDK_SCIO:
		logger.Errorf("RunCode(): unimplemented sdk: %s\n", info.Sdk)
		return nil, errors.InvalidArgumentError("Run code()", "unimplemented sdk: "+info.Sdk.String())
	}

	cacheExpirationTime := controller.env.ApplicationEnvs.CacheEnvs().KeyExpirationTime()
	pipelineId := uuid.New()

	lc, err := life_cycle.Setup(info.Sdk, info.Code, pipelineId, controller.env.ApplicationEnvs.WorkingDir(), controller.env.BeamSdkEnvs.PreparedModDir())
	if err != nil {
		logger.Errorf("RunCode(): error during setup file system: %s\n", err.Error())
		return nil, errors.InternalError("Run code", "Error during setup file system: "+err.Error())
	}

	compileBuilder, err := compile_builder.Setup(lc.GetAbsoluteSourceFilePath(), lc.GetAbsoluteBaseFolderPath(), info.Sdk, controller.env.BeamSdkEnvs.ExecutorConfig)
	if err != nil {
		logger.Errorf("RunCode(): error during setup run builder: %s\n", err.Error())
		code_processing.DeleteFolders(pipelineId, lc)
		return nil, errors.InvalidArgumentError("Run code", "Error during setup compile builder: "+err.Error())
	}

	if err = utils.SetToCache(ctx, controller.cacheService, pipelineId, cache.Status, pb.Status_STATUS_VALIDATING); err != nil {
		code_processing.DeleteFolders(pipelineId, lc)
		return nil, errors.InternalError("Run code()", "Error during set value to cache: "+err.Error())
	}
	if err = utils.SetToCache(ctx, controller.cacheService, pipelineId, cache.RunOutputIndex, 0); err != nil {
		return nil, errors.InternalError("Run code()", "Error during set value to cache: "+err.Error())
	}
	if err = controller.cacheService.SetExpTime(ctx, pipelineId, cacheExpirationTime); err != nil {
		logger.Errorf("%s: RunCode(): cache.SetExpTime(): %s\n", pipelineId, err.Error())
		code_processing.DeleteFolders(pipelineId, lc)
		return nil, errors.InternalError("Run code()", "Error during set expiration to cache: "+err.Error())
	}

	// TODO change using of context.TODO() to context.Background()
	go code_processing.Process(context.TODO(), controller.cacheService, lc, compileBuilder, pipelineId, &controller.env.ApplicationEnvs, &controller.env.BeamSdkEnvs)

	pipelineInfo := pb.RunCodeResponse{PipelineUuid: pipelineId.String()}
	return &pipelineInfo, nil
}

// CheckStatus is checking status for the specific pipeline by PipelineUuid
func (controller *playgroundController) CheckStatus(ctx context.Context, info *pb.CheckStatusRequest) (*pb.CheckStatusResponse, error) {
	pipelineId, err := uuid.Parse(info.PipelineUuid)
	if err != nil {
		logger.Errorf("%s: CheckStatus(): pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid, err.Error())
		return nil, errors.InvalidArgumentError("CheckStatus", "pipelineId has incorrect value and couldn't be parsed as uuid value: "+info.PipelineUuid)
	}
	status, err := code_processing.GetProcessingStatus(ctx, controller.cacheService, pipelineId, "CheckStatus")
	if err != nil {
		return nil, err
	}
	return &pb.CheckStatusResponse{Status: status}, nil
}

// GetRunOutput is returning output of execution for specific pipeline by PipelineUuid
func (controller *playgroundController) GetRunOutput(ctx context.Context, info *pb.GetRunOutputRequest) (*pb.GetRunOutputResponse, error) {
	pipelineId, err := uuid.Parse(info.PipelineUuid)
	if err != nil {
		logger.Errorf("%s: GetRunOutput(): pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid, err.Error())
		return nil, errors.InvalidArgumentError("GetRunOutput", "pipelineId has incorrect value and couldn't be parsed as uuid value: "+info.PipelineUuid)
	}
	lastIndex, err := code_processing.GetRunOutputLastIndex(ctx, controller.cacheService, pipelineId, "GetRunOutput")
	if err != nil {
		return nil, err
	}
	runOutput, err := code_processing.GetProcessingOutput(ctx, controller.cacheService, pipelineId, cache.RunOutput, "GetRunOutput")
	if err != nil {
		return nil, err
	}
	newRunOutput := ""
	if len(runOutput) > lastIndex {
		newRunOutput = runOutput[lastIndex:]
		utils.SetToCache(ctx, controller.cacheService, pipelineId, cache.RunOutputIndex, lastIndex+len(newRunOutput))
	}

	pipelineResult := pb.GetRunOutputResponse{Output: newRunOutput}

	return &pipelineResult, nil
}

// GetRunError is returning error output of execution for specific pipeline by PipelineUuid
func (controller *playgroundController) GetRunError(ctx context.Context, info *pb.GetRunErrorRequest) (*pb.GetRunErrorResponse, error) {
	pipelineId, err := uuid.Parse(info.PipelineUuid)
	if err != nil {
		logger.Errorf("%s: GetRunError(): pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid, err.Error())
		return nil, errors.InvalidArgumentError("GetRunError", "pipelineId has incorrect value and couldn't be parsed as uuid value: "+info.PipelineUuid)
	}
	runError, err := code_processing.GetProcessingOutput(ctx, controller.cacheService, pipelineId, cache.RunError, "GetRunError")
	if err != nil {
		return nil, err
	}
	return &pb.GetRunErrorResponse{Output: runError}, nil
}

//GetCompileOutput is returning output of compilation for specific pipeline by PipelineUuid
func (controller *playgroundController) GetCompileOutput(ctx context.Context, info *pb.GetCompileOutputRequest) (*pb.GetCompileOutputResponse, error) {
	pipelineId, err := uuid.Parse(info.PipelineUuid)
	if err != nil {
		logger.Errorf("%s: GetCompileOutput(): pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid, err.Error())
		return nil, errors.InvalidArgumentError("GetCompileOutput", "pipelineId has incorrect value and couldn't be parsed as uuid value: "+info.PipelineUuid)
	}
	compileOutput, err := code_processing.GetProcessingOutput(ctx, controller.cacheService, pipelineId, cache.CompileOutput, "GetCompileOutput")
	if err != nil {
		return nil, err
	}
	return &pb.GetCompileOutputResponse{Output: compileOutput}, nil
}

// Cancel is setting cancel flag to stop code processing
func (controller *playgroundController) Cancel(ctx context.Context, info *pb.CancelRequest) (*pb.CancelResponse, error) {
	pipelineId, err := uuid.Parse(info.PipelineUuid)
	if err != nil {
		logger.Errorf("%s: Cancel(): pipelineId has incorrect value and couldn't be parsed as uuid value: %s", info.PipelineUuid, err.Error())
		return nil, errors.InvalidArgumentError("Cancel", "pipelineId has incorrect value and couldn't be parsed as uuid value: "+info.PipelineUuid)
	}
	if err := utils.SetToCache(ctx, controller.cacheService, pipelineId, cache.Canceled, true); err != nil {
		return nil, errors.InternalError("Cancel", "error during set cancel flag to cache")
	}
	return &pb.CancelResponse{}, nil
}

// GetPrecompiledObjects returns the list of examples
func (controller *playgroundController) GetPrecompiledObjects(ctx context.Context, info *pb.GetPrecompiledObjectsRequest) (*pb.GetPrecompiledObjectsResponse, error) {
	bucket := cloud_bucket.New()
	sdkToCategories, err := bucket.GetPrecompiledObjects(ctx, info.Sdk, info.Category)
	if err != nil {
		logger.Errorf("GetPrecompiledObjects(): cloud storage error: %s", err.Error())
		return nil, errors.InternalError("GetPrecompiledObjects(): ", err.Error())
	}
	response := pb.GetPrecompiledObjectsResponse{SdkCategories: make([]*pb.Categories, 0)}
	for sdkName, categories := range *sdkToCategories {
		sdkCategory := pb.Categories{Sdk: pb.Sdk(pb.Sdk_value[sdkName]), Categories: make([]*pb.Categories_Category, 0)}
		for categoryName, precompiledObjects := range categories {
			utils.PutPrecompiledObjectsToCategory(categoryName, &precompiledObjects, &sdkCategory)
		}
		response.SdkCategories = append(response.SdkCategories, &sdkCategory)
	}
	return &response, nil
}

// GetPrecompiledObjectCode returns the code of the specific example
func (controller *playgroundController) GetPrecompiledObjectCode(ctx context.Context, info *pb.GetPrecompiledObjectRequest) (*pb.GetPrecompiledObjectCodeResponse, error) {
	cd := cloud_bucket.New()
	codeString, err := cd.GetPrecompiledObject(ctx, info.GetCloudPath())
	if err != nil {
		logger.Errorf("GetPrecompiledObject(): cloud storage error: %s", err.Error())
		return nil, errors.InternalError("GetPrecompiledObjects(): ", err.Error())
	}
	response := pb.GetPrecompiledObjectCodeResponse{Code: *codeString}
	return &response, nil
}

// GetPrecompiledObjectOutput returns the output of the compiled and run example
func (controller *playgroundController) GetPrecompiledObjectOutput(ctx context.Context, info *pb.GetPrecompiledObjectRequest) (*pb.GetRunOutputResponse, error) {
	cd := cloud_bucket.New()
	output, err := cd.GetPrecompiledObjectOutput(ctx, info.GetCloudPath())
	if err != nil {
		logger.Errorf("GetPrecompiledObjectOutput(): cloud storage error: %s", err.Error())
		return nil, errors.InternalError("GetPrecompiledObjectOutput(): ", err.Error())
	}
	response := pb.GetRunOutputResponse{Output: *output}
	return &response, nil
}
