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
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/errors"
	"beam.apache.org/playground/backend/internal/executors"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"beam.apache.org/playground/backend/internal/validators"
	"context"
	"github.com/google/uuid"
	"log"
)

type playgroundController struct {
	env          *environment.Environment
	cacheService cache.Cache

	pb.UnimplementedPlaygroundServiceServer
}

//RunCode is running code from requests using a particular SDK
func (controller *playgroundController) RunCode(ctx context.Context, info *pb.RunCodeRequest) (*pb.RunCodeResponse, error) {
	// check for correct sdk
	switch info.Sdk {
	case pb.Sdk_SDK_UNSPECIFIED, pb.Sdk_SDK_GO, pb.Sdk_SDK_PYTHON, pb.Sdk_SDK_SCIO:
		log.Printf("RunCode(): unimplemented sdk: %s\n", info.Sdk)
		return nil, errors.InvalidArgumentError("Run code()", "unimplemented sdk: "+info.Sdk.String())
	}

	pipelineId := uuid.New()

	defer func() {
		log.Printf("RunCode() is completed for pipeline with id: %s\n", pipelineId)
	}()

	cacheExpirationTime := controller.env.ApplicationEnvs.CacheEnvs().KeyExpirationTime()

	// create file system service
	lc, err := fs_tool.NewLifeCycle(info.Sdk, pipelineId, controller.env.ApplicationEnvs.WorkingDir())
	if err != nil {
		log.Printf("%s: RunCode(): NewLifeCycle(): %s\n", pipelineId, err.Error())
		return nil, errors.InternalError("Run code", "Error during creating file system service: "+err.Error())
	}

	// create folders
	err = lc.CreateFolders()
	if err != nil {
		log.Printf("%s: RunCode(): CreateFolders(): %s\n", pipelineId, err.Error())
		return nil, errors.InternalError("Run code()", "Error during preparing folders: "+err.Error())
	}

	// create file with code
	_, err = lc.CreateExecutableFile(info.Code)
	if err != nil {
		log.Printf("%s: RunCode(): CreateExecutableFile(): %s\n", pipelineId, err.Error())
		return nil, errors.InternalError("Run code()", "Error during creating file with code: "+err.Error())
	}

	filePath := lc.GetAbsoluteExecutableFilePath()
	workingDir := lc.GetAbsoluteExecutableFilesFolderPath()

	// create executor
	val := setupValidators(info.Sdk, filePath)
	exec := executors.NewExecutorBuilder().
		WithValidator().
		WithSdkValidators(val).
		WithCompiler().
		WithCommand(controller.env.BeamSdkEnvs.ExecutorConfig.CompileCmd).
		WithArgs(controller.env.BeamSdkEnvs.ExecutorConfig.CompileArgs).
		WithFileName(filePath).
		WithWorkingDir(workingDir)

	setToCache(ctx, controller.cacheService, pipelineId, cache.Status, pb.Status_STATUS_EXECUTING)

	err = controller.cacheService.SetExpTime(ctx, pipelineId, cacheExpirationTime)
	if err != nil {
		log.Printf("%s: RunCode(): cache.SetExpTime(): %s\n", pipelineId, err.Error())
		return nil, errors.InternalError("Run code()", "Error during set expiration to cache: "+err.Error())
	}

	go processCode(ctx, controller.cacheService, lc, exec, pipelineId, controller.env)

	pipelineInfo := pb.RunCodeResponse{PipelineUuid: pipelineId.String()}
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

// setupValidators returns validators based on sdk
func setupValidators(sdk pb.Sdk, filepath string) *[]validators.Validator {
	var val *[]validators.Validator
	switch sdk {
	case pb.Sdk_SDK_JAVA:
		val = validators.GetJavaValidators(filepath)
	}
	return val
}

// processCode validates, compiles and runs code by pipelineId.
// During each operation updates status of execution and saves it into cache.
// In case of some step is failed saves output logs to cache.
// After success code running saves output to cache.
// At the end of this method deletes all created folders
func processCode(ctx context.Context, cacheService cache.Cache, lc *fs_tool.LifeCycle, execBuilder *executors.CompileBuilder, pipelineId uuid.UUID, env *environment.Environment) {
	defer cleanUp(pipelineId, lc)

	exec := execBuilder.Build()

	// validate
	log.Printf("%s: Validate() ...\n", pipelineId)

	validateFunc := exec.Validate()
	if err := validateFunc(); err != nil {
		// error during validation
		// TODO move to processError when status for validation error will be added
		log.Printf("%s: Validate: %s\n", pipelineId, err.Error())
		setToCache(ctx, cacheService, pipelineId, cache.Status, pb.Status_STATUS_ERROR)
		return
	}
	log.Printf("%s: Validate() finish\n", pipelineId)

	// compile
	log.Printf("%s: Compile() ...\n", pipelineId)
	compileCmd := exec.Compile()
	if data, err := compileCmd.CombinedOutput(); err != nil {
		processError(ctx, err, data, pipelineId, cacheService, pb.Status_STATUS_COMPILE_ERROR)
		return
	}
	log.Printf("%s: Compile() finish\n", pipelineId)

	// set empty value to pipelineId: cache.SubKey_CompileOutput
	setToCache(ctx, cacheService, pipelineId, cache.CompileOutput, "")

	className, err := lc.ExecutableName(pipelineId, env.ApplicationEnvs.WorkingDir())
	if err != nil {
		log.Printf("%s: get executable file name: %s\n", pipelineId, err.Error())
		setToCache(ctx, cacheService, pipelineId, cache.Status, pb.Status_STATUS_ERROR)
		return
	}

	exec = execBuilder.
		WithRunner().
		WithCommand(env.BeamSdkEnvs.ExecutorConfig.RunCmd).
		WithArgs(env.BeamSdkEnvs.ExecutorConfig.RunArgs).
		WithClassName(className).
		WithWorkingDir(lc.GetAbsoluteExecutableFilesFolderPath()).
		Build()

	log.Printf("%s: Run() ...\n", pipelineId)
	runCmd := exec.Run()
	data, err := runCmd.CombinedOutput()
	if err != nil {
		// error during run code
		processError(ctx, err, data, pipelineId, cacheService, pb.Status_STATUS_ERROR)
		return
	}
	log.Printf("%s: Run() finish\n", pipelineId)
	processSuccess(ctx, data, pipelineId, cacheService)
}

// cleanUp removes all prepared folders for received LifeCycle
func cleanUp(pipelineId uuid.UUID, lc *fs_tool.LifeCycle) {
	log.Printf("%s: DeleteFolders() ...\n", pipelineId)
	err := lc.DeleteFolders()
	if err != nil {
		log.Printf("%s: DeleteFolders(): %s\n", pipelineId, err.Error())
	}
	log.Printf("%s: DeleteFolders() complete\n", pipelineId)
	log.Printf("%s: complete\n", pipelineId)
}

// processError processes error received during processing code via setting a corresponding status and output to cache
func processError(ctx context.Context, err error, data []byte, pipelineId uuid.UUID, cacheService cache.Cache, status pb.Status) {
	switch status {
	case pb.Status_STATUS_ERROR:
		log.Printf("%s: Run: err: %s, output: %s\n", pipelineId, err.Error(), data)

		// set to cache pipelineId: cache.SubKey_RunOutput: err.Error()
		setToCache(ctx, cacheService, pipelineId, cache.RunOutput, "error: "+err.Error()+", output: "+string(data))

		// set to cache pipelineId: cache.SubKey_Status: pb.Status_STATUS_ERROR
		setToCache(ctx, cacheService, pipelineId, cache.Status, pb.Status_STATUS_ERROR)
	case pb.Status_STATUS_COMPILE_ERROR:
		log.Printf("%s: Compile: err: %s, output: %s\n", pipelineId, err.Error(), data)

		// set to cache pipelineId: cache.SubKey_CompileOutput: err.Error()
		setToCache(ctx, cacheService, pipelineId, cache.CompileOutput, "error: "+err.Error()+", output: "+string(data))

		// set to cache pipelineId: cache.SubKey_Status: pb.Status_STATUS_ERROR
		setToCache(ctx, cacheService, pipelineId, cache.Status, pb.Status_STATUS_COMPILE_ERROR)
	}
}

// processSuccess processes case after successful code processing via setting a corresponding status and output to cache
func processSuccess(ctx context.Context, output []byte, pipelineId uuid.UUID, cacheService cache.Cache) {
	// set to cache pipelineId: cache.SubKey_RunOutput: output
	setToCache(ctx, cacheService, pipelineId, cache.RunOutput, string(output))

	// set to cache pipelineId: cache.SubKey_Status: pb.Status_STATUS_FINISHED
	setToCache(ctx, cacheService, pipelineId, cache.Status, pb.Status_STATUS_FINISHED)
}

// setToCache puts value to cache by key and subKey
func setToCache(ctx context.Context, cacheService cache.Cache, key uuid.UUID, subKey cache.SubKey, value interface{}) {
	err := cacheService.SetValue(ctx, key, subKey, value)
	if err != nil {
		log.Printf("%s: cache.SetValue: %s\n", key, err.Error())
	}
}
