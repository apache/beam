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
	"beam.apache.org/playground/backend/internal/logger"
	"beam.apache.org/playground/backend/internal/validators"
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
	switch info.Sdk {
	case pb.Sdk_SDK_UNSPECIFIED, pb.Sdk_SDK_GO, pb.Sdk_SDK_PYTHON, pb.Sdk_SDK_SCIO:
		logger.Errorf("RunCode(): unimplemented sdk: %s\n", info.Sdk)
		return nil, errors.InvalidArgumentError("Run code()", "unimplemented sdk: "+info.Sdk.String())
	}

	cacheExpirationTime := controller.env.ApplicationEnvs.CacheEnvs().KeyExpirationTime()
	pipelineId := uuid.New()

	lc, err := setupLifeCycle(info.Sdk, info.Code, pipelineId, controller.env.ApplicationEnvs.WorkingDir())
	if err != nil {
		return nil, errors.InternalError("Run code", "Error during setup file system: "+err.Error())
	}

	compileBuilder := setupCompileBuilder(lc, info.Sdk, controller.env.BeamSdkEnvs.ExecutorConfig)

	setToCache(ctx, controller.cacheService, pipelineId, cache.Status, pb.Status_STATUS_EXECUTING)
	if err := controller.cacheService.SetExpTime(ctx, pipelineId, cacheExpirationTime); err != nil {
		logger.Errorf("%s: RunCode(): cache.SetExpTime(): %s\n", pipelineId, err.Error())
		return nil, errors.InternalError("Run code()", "Error during set expiration to cache: "+err.Error())
	}

	go processCode(ctx, controller.cacheService, lc, compileBuilder, pipelineId, controller.env, info.Sdk)

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
	pipelineId := info.PipelineUuid
	compileOutputInterface, err := controller.cacheService.GetValue(ctx, uuid.MustParse(pipelineId), cache.CompileOutput)
	if err != nil {
		logger.Errorf("%s: GetCompileOutput(): cache.GetValue: error: %s", pipelineId, err.Error())
		return nil, errors.NotFoundError("GetCompileOutput", "there is no compile output for pipelineId: "+pipelineId+", subKey: cache.SubKey_CompileOutput")
	}
	compileOutput, converted := compileOutputInterface.(string)
	if !converted {
		return nil, errors.InternalError("GetCompileOutput", "compile output can't be converted to string")
	}
	pipelineResult := pb.GetCompileOutputResponse{Output: compileOutput}

	return &pipelineResult, nil
}

// setupLifeCycle creates fs_tool.LifeCycle and prepares files and folders needed to code processing
func setupLifeCycle(sdk pb.Sdk, code string, pipelineId uuid.UUID, workingDir string) (*fs_tool.LifeCycle, error) {
	// create file system service
	lc, err := fs_tool.NewLifeCycle(sdk, pipelineId, workingDir)
	if err != nil {
		logger.Errorf("%s: RunCode(): NewLifeCycle(): %s\n", pipelineId, err.Error())
		return nil, err
	}

	// create folders
	err = lc.CreateFolders()
	if err != nil {
		logger.Errorf("%s: RunCode(): CreateFolders(): %s\n", pipelineId, err.Error())
		return nil, err
	}

	// create file with code
	_, err = lc.CreateExecutableFile(code)
	if err != nil {
		logger.Errorf("%s: RunCode(): CreateExecutableFile(): %s\n", pipelineId, err.Error())
		return nil, err
	}
	return lc, nil
}

// setupCompileBuilder returns executors.CompileBuilder with validators and compiler based on sdk
func setupCompileBuilder(lc *fs_tool.LifeCycle, sdk pb.Sdk, executorConfig *environment.ExecutorConfig) *executors.CompileBuilder {
	filePath := lc.GetAbsoluteExecutableFilePath()
	val := setupValidators(sdk, filePath)

	compileBuilder := executors.NewExecutorBuilder().
		WithValidator().
		WithSdkValidators(val).
		WithCompiler()

	switch sdk {
	case pb.Sdk_SDK_JAVA:
		workingDir := lc.GetAbsoluteExecutableFilesFolderPath()

		compileBuilder = compileBuilder.
			WithCommand(executorConfig.CompileCmd).
			WithArgs(executorConfig.CompileArgs).
			WithFileName(filePath).
			WithWorkingDir(workingDir)
	}
	return compileBuilder
}

// setupRunBuilder returns executors.RunBuilder based on sdk
func setupRunBuilder(pipelineId uuid.UUID, lc *fs_tool.LifeCycle, sdk pb.Sdk, env *environment.Environment, compileBuilder *executors.CompileBuilder) (*executors.RunBuilder, error) {
	runBuilder := compileBuilder.
		WithRunner().
		WithCommand(env.BeamSdkEnvs.ExecutorConfig.RunCmd).
		WithArgs(env.BeamSdkEnvs.ExecutorConfig.RunArgs).
		WithWorkingDir(lc.GetAbsoluteExecutableFilesFolderPath())

	switch sdk {
	case pb.Sdk_SDK_JAVA:
		className, err := lc.ExecutableName(pipelineId, env.ApplicationEnvs.WorkingDir())
		if err != nil {
			logger.Errorf("%s: get executable file name: %s\n", pipelineId, err.Error())
			return nil, err
		}

		runBuilder = runBuilder.
			WithClassName(className)
	}
	return runBuilder, nil
}

// setupValidators returns slice of validators.Validator based on sdk
func setupValidators(sdk pb.Sdk, filepath string) *[]validators.Validator {
	var val *[]validators.Validator
	switch sdk {
	case pb.Sdk_SDK_JAVA:
		val = validators.GetJavaValidators(filepath)
	}
	return val
}

// processCode validates, compiles and runs code by pipelineId.
// During each operation updates status of execution and saves it into cache:
// - In case of validation step is failed saves playground.Status_STATUS_ERROR as cache.Status into cache.
// - In case of compile step is failed saves playground.Status_STATUS_COMPILE_ERROR as cache.Status and compile logs as cache.CompileOutput into cache.
// - In case of compile step is completed with no errors saves empty string ("") as cache.CompileOutput into cache.
// - In case of run step is failed saves playground.Status_STATUS_ERROR as cache.Status and run logs as cache.RunOutput into cache.
// - In case of run step is completed with no errors saves playground.Status_STATUS_FINISHED as cache.Status and run output as cache.RunOutput into cache.
// At the end of this method deletes all created folders.
func processCode(ctx context.Context, cacheService cache.Cache, lc *fs_tool.LifeCycle, compileBuilder *executors.CompileBuilder, pipelineId uuid.UUID, env *environment.Environment, sdk pb.Sdk) {
	defer cleanUp(pipelineId, lc)

	// build executor for validate and compile steps
	exec := compileBuilder.Build()

	// validate
	logger.Infof("%s: Validate() ...\n", pipelineId)
	validateFunc := exec.Validate()
	if err := validateFunc(); err != nil {
		// error during validation
		// TODO move to processError when status for validation error will be added
		logger.Errorf("%s: Validate: %s\n", pipelineId, err.Error())
		setToCache(ctx, cacheService, pipelineId, cache.Status, pb.Status_STATUS_ERROR)
		return
	}
	logger.Infof("%s: Validate() finish\n", pipelineId)

	// compile
	logger.Infof("%s: Compile() ...\n", pipelineId)
	compileCmd := exec.Compile()
	if data, err := compileCmd.CombinedOutput(); err != nil {
		processError(ctx, err, data, pipelineId, cacheService, pb.Status_STATUS_COMPILE_ERROR)
		return
	} else {
		processSuccess(ctx, data, pipelineId, cacheService, pb.Status_STATUS_EXECUTING)
	}

	runBuilder, err := setupRunBuilder(pipelineId, lc, sdk, env, compileBuilder)
	if err != nil {
		logger.Errorf("%s: error during setup runBuilder: %s\n", pipelineId, err.Error())
		setToCache(ctx, cacheService, pipelineId, cache.Status, pb.Status_STATUS_ERROR)
		return
	}

	// build executor for run step
	exec = runBuilder.Build()

	logger.Infof("%s: Run() ...\n", pipelineId)
	runCmd := exec.Run()
	if data, err := runCmd.CombinedOutput(); err != nil {
		// error during run code
		processError(ctx, err, data, pipelineId, cacheService, pb.Status_STATUS_ERROR)
		return
	} else {
		processSuccess(ctx, data, pipelineId, cacheService, pb.Status_STATUS_FINISHED)
	}
}

// cleanUp removes all prepared folders for received LifeCycle
func cleanUp(pipelineId uuid.UUID, lc *fs_tool.LifeCycle) {
	logger.Infof("%s: DeleteFolders() ...\n", pipelineId)
	if err := lc.DeleteFolders(); err != nil {
		logger.Error("%s: DeleteFolders(): %s\n", pipelineId, err.Error())
	}
	logger.Infof("%s: DeleteFolders() complete\n", pipelineId)
	logger.Infof("%s: complete\n", pipelineId)
}

// processError processes error received during processing code via setting a corresponding status and output to cache
func processError(ctx context.Context, err error, data []byte, pipelineId uuid.UUID, cacheService cache.Cache, status pb.Status) {
	switch status {
	case pb.Status_STATUS_ERROR:
		logger.Errorf("%s: Run: err: %s, output: %s\n", pipelineId, err.Error(), data)

		// set to cache pipelineId: cache.SubKey_RunOutput: err.Error()
		setToCache(ctx, cacheService, pipelineId, cache.RunOutput, "error: "+err.Error()+", output: "+string(data))

		// set to cache pipelineId: cache.SubKey_Status: pb.Status_STATUS_ERROR
		setToCache(ctx, cacheService, pipelineId, cache.Status, pb.Status_STATUS_ERROR)
	case pb.Status_STATUS_COMPILE_ERROR:
		logger.Errorf("%s: Compile: err: %s, output: %s\n", pipelineId, err.Error(), data)

		// set to cache pipelineId: cache.SubKey_CompileOutput: err.Error()
		setToCache(ctx, cacheService, pipelineId, cache.CompileOutput, "error: "+err.Error()+", output: "+string(data))

		// set to cache pipelineId: cache.SubKey_Status: pb.Status_STATUS_ERROR
		setToCache(ctx, cacheService, pipelineId, cache.Status, pb.Status_STATUS_COMPILE_ERROR)
	}
}

// processSuccess processes case after successful code processing via setting a corresponding status and output to cache
func processSuccess(ctx context.Context, output []byte, pipelineId uuid.UUID, cacheService cache.Cache, status pb.Status) {
	switch status {
	case pb.Status_STATUS_EXECUTING:
		logger.Infof("%s: Compile() finish\n", pipelineId)

		// set to cache pipelineId: cache.SubKey_CompileOutput: output
		setToCache(ctx, cacheService, pipelineId, cache.CompileOutput, string(output))

		// set to cache pipelineId: cache.SubKey_Status: pb.Status_STATUS_EXECUTING
		setToCache(ctx, cacheService, pipelineId, cache.Status, pb.Status_STATUS_EXECUTING)
	case pb.Status_STATUS_FINISHED:
		logger.Infof("%s: Run() finish\n", pipelineId)

		// set to cache pipelineId: cache.SubKey_RunOutput: output
		setToCache(ctx, cacheService, pipelineId, cache.RunOutput, string(output))

		// set to cache pipelineId: cache.SubKey_Status: pb.Status_STATUS_FINISHED
		setToCache(ctx, cacheService, pipelineId, cache.Status, pb.Status_STATUS_FINISHED)
	}
}

// setToCache puts value to cache by key and subKey
func setToCache(ctx context.Context, cacheService cache.Cache, key uuid.UUID, subKey cache.SubKey, value interface{}) {
	if err := cacheService.SetValue(ctx, key, subKey, value); err != nil {
		logger.Errorf("%s: cache.SetValue: %s\n", key, err.Error())
	}
}
