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
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/errors"
	"beam.apache.org/playground/backend/internal/executors"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"beam.apache.org/playground/backend/internal/logger"
	"beam.apache.org/playground/backend/internal/setup_tools/run_builder"
	"beam.apache.org/playground/backend/internal/streaming"
	"bytes"
	"context"
	"fmt"
	"github.com/google/uuid"
	"os/exec"
	"time"
)

// Process validates, compiles and runs code by pipelineId.
// During each operation updates status of execution and saves it into cache:
// - In case of processing works more that timeout duration saves playground.Status_STATUS_RUN_TIMEOUT as cache.Status into cache.
// - In case of code processing has been canceled saves playground.Status_STATUS_CANCELED as cache.Status into cache.
// - In case of validation step is failed saves playground.Status_STATUS_VALIDATION_ERROR as cache.Status into cache.
// - In case of compile step is failed saves playground.Status_STATUS_COMPILE_ERROR as cache.Status and compile logs as cache.CompileOutput into cache.
// - In case of compile step is completed with no errors saves compile output as cache.CompileOutput into cache.
// - In case of run step is failed saves playground.Status_STATUS_RUN_ERROR as cache.Status and run logs as cache.RunError into cache.
// - In case of run step is completed with no errors saves playground.Status_STATUS_FINISHED as cache.Status and run output as cache.RunOutput into cache.
// At the end of this method deletes all created folders.
func Process(ctx context.Context, cacheService cache.Cache, lc *fs_tool.LifeCycle, compileBuilder *executors.CompileBuilder, pipelineId uuid.UUID, appEnv *environment.ApplicationEnvs, sdkEnv *environment.BeamEnvs) {
	ctxWithTimeout, finishCtxFunc := context.WithTimeout(ctx, appEnv.PipelineExecuteTimeout())
	defer func(lc *fs_tool.LifeCycle) {
		finishCtxFunc()
		DeleteFolders(pipelineId, lc)
	}(lc)

	errorChannel := make(chan error, 1)
	dataChannel := make(chan interface{}, 1)
	successChannel := make(chan bool, 1)
	cancelChannel := make(chan bool, 1)

	go cancelCheck(ctxWithTimeout, pipelineId, cancelChannel, cacheService)

	// build executor for validate and compile steps
	executor := compileBuilder.Build()

	// validate
	logger.Infof("%s: Validate() ...\n", pipelineId)
	validateFunc := executor.Validate()
	go validateFunc(successChannel, errorChannel)

	if err := processStep(ctxWithTimeout, pipelineId, cacheService, cancelChannel, successChannel, nil, nil, errorChannel, pb.Status_STATUS_VALIDATION_ERROR, pb.Status_STATUS_PREPARING); err != nil {
		return
	}

	// prepare
	logger.Infof("%s: Prepare() ...\n", pipelineId)
	prepareFunc := executor.Prepare()
	go prepareFunc(successChannel, errorChannel)

	if err := processStep(ctxWithTimeout, pipelineId, cacheService, cancelChannel, successChannel, nil, nil, errorChannel, pb.Status_STATUS_PREPARATION_ERROR, pb.Status_STATUS_COMPILING); err != nil {
		return
	}

	// compile
	logger.Infof("%s: Compile() ...\n", pipelineId)
	compileCmd := executor.Compile(ctxWithTimeout)
	go func(cmd *exec.Cmd, successChannel chan bool, errChannel chan error, dataChannel chan interface{}) {
		// TODO separate stderr from stdout [BEAM-13208]
		data, err := cmd.CombinedOutput()
		dataChannel <- data
		if err != nil {
			errChannel <- err
			successChannel <- false
		} else {
			successChannel <- true
		}
	}(compileCmd, successChannel, errorChannel, dataChannel)

	if err := processStep(ctxWithTimeout, pipelineId, cacheService, cancelChannel, successChannel, dataChannel, nil, errorChannel, pb.Status_STATUS_COMPILE_ERROR, pb.Status_STATUS_EXECUTING); err != nil {
		return
	}

	runBuilder, err := run_builder.Setup(pipelineId, lc, appEnv.WorkingDir(), sdkEnv, compileBuilder)
	if err != nil {
		logger.Errorf("%s: error during setup run builder: %s\n", pipelineId, err.Error())
		cacheService.SetValue(ctxWithTimeout, pipelineId, cache.Status, pb.Status_STATUS_ERROR)
		return
	}

	// build executor for run step
	executor = runBuilder.Build()

	// run
	logger.Infof("%s: Run() ...\n", pipelineId)
	runCmd := executor.Run(ctxWithTimeout)

	var runError bytes.Buffer
	runOutput := &streaming.RunOutputWriter{Ctx: ctxWithTimeout, CacheService: cacheService, PipelineId: pipelineId}
	runCmd.Stderr = &runError
	runCmd.Stdout = runOutput
	go func(cmd *exec.Cmd, successChannel chan bool, errChannel chan error, dataChannel chan interface{}) {
		err := cmd.Run()
		if err != nil {
			errChannel <- err
			successChannel <- false
		} else {
			successChannel <- true
		}
	}(runCmd, successChannel, errorChannel, dataChannel)

	processStep(ctxWithTimeout, pipelineId, cacheService, cancelChannel, successChannel, nil, &runError, errorChannel, pb.Status_STATUS_RUN_ERROR, pb.Status_STATUS_FINISHED)
}

// GetProcessingOutput gets processing output value from cache by key and subKey.
// In case key doesn't exist in cache - returns an errors.NotFoundError.
// In case subKey doesn't exist in cache for the key - returns an errors.NotFoundError.
// In case value from cache by key and subKey couldn't be converted to string - returns an errors.InternalError.
func GetProcessingOutput(ctx context.Context, cacheService cache.Cache, key uuid.UUID, subKey cache.SubKey, errorTitle string) (string, error) {
	value, err := cacheService.GetValue(ctx, key, subKey)
	if err != nil {
		logger.Errorf("%s: GetStringValueFromCache(): cache.GetValue: error: %s", key, err.Error())
		return "", errors.NotFoundError(errorTitle, fmt.Sprintf("Error during getting cache by key: %s, subKey: %s", key.String(), string(subKey)))
	}
	stringValue, converted := value.(string)
	if !converted {
		logger.Errorf("%s: couldn't convert value to string: %s", key, value)
		return "", errors.InternalError(errorTitle, fmt.Sprintf("Value from cache couldn't be converted to string: %s", value))
	}
	return stringValue, nil
}

// GetProcessingStatus gets processing status from cache by key.
// In case key doesn't exist in cache - returns an errors.NotFoundError.
// In case value from cache by key and subKey couldn't be converted to playground.Status - returns an errors.InternalError.
func GetProcessingStatus(ctx context.Context, cacheService cache.Cache, key uuid.UUID, errorTitle string) (pb.Status, error) {
	value, err := cacheService.GetValue(ctx, key, cache.Status)
	if err != nil {
		logger.Errorf("%s: GetStringValueFromCache(): cache.GetValue: error: %s", key, err.Error())
		return pb.Status_STATUS_UNSPECIFIED, errors.NotFoundError(errorTitle, fmt.Sprintf("Error during getting cache by key: %s, subKey: %s", key.String(), string(cache.Status)))
	}
	statusValue, converted := value.(pb.Status)
	if !converted {
		logger.Errorf("%s: couldn't convert value to correct status enum: %s", key, value)
		return pb.Status_STATUS_UNSPECIFIED, errors.InternalError(errorTitle, fmt.Sprintf("Value from cache couldn't be converted to correct status enum: %s", value))
	}
	return statusValue, nil
}

// GetRunOutputLastIndex gets run output's last index from cache by key.
// In case key doesn't exist in cache - returns an errors.NotFoundError.
// In case value from cache by key and subKey couldn't be converted to int - returns an errors.InternalError.
func GetRunOutputLastIndex(ctx context.Context, cacheService cache.Cache, key uuid.UUID, errorTitle string) (int, error) {
	value, err := cacheService.GetValue(ctx, key, cache.RunOutputIndex)
	if err != nil {
		logger.Errorf("%s: GetStringValueFromCache(): cache.GetValue: error: %s", key, err.Error())
		return 0, errors.NotFoundError(errorTitle, fmt.Sprintf("Error during getting cache by key: %s, subKey: %s", key.String(), string(cache.RunOutputIndex)))
	}
	intValue, converted := value.(int)
	if !converted {
		logger.Errorf("%s: couldn't convert value to string: %s", key, value)
		return 0, errors.InternalError(errorTitle, fmt.Sprintf("Value from cache couldn't be converted to int: %s", value))
	}
	return intValue, nil
}

// processStep processes each executor's step with cancel and timeout checks.
// If finishes by canceling, timeout or error - returns error.
// If finishes successfully returns nil.
func processStep(ctx context.Context, pipelineId uuid.UUID, cacheService cache.Cache, cancelChannel, successChannel chan bool, dataChannel chan interface{}, errorDataBuffer *bytes.Buffer, errorChannel chan error, errorCaseStatus, successCaseStatus pb.Status) error {
	select {
	case <-ctx.Done():
		finishByTimeout(ctx, pipelineId, cacheService)
		return fmt.Errorf("%s: context was done", pipelineId)
	case <-cancelChannel:
		processCancel(ctx, cacheService, pipelineId)
		return fmt.Errorf("%s: code processing was canceled", pipelineId)
	case ok := <-successChannel:
		var data []byte = nil
		if dataChannel != nil {
			temp := <-dataChannel
			data = temp.([]byte)
		}
		if !ok {
			err := <-errorChannel
			var errorData = data
			if errorDataBuffer != nil {
				errorData = errorDataBuffer.Bytes()
			}
			processError(ctx, err, errorData, pipelineId, cacheService, errorCaseStatus)
			return fmt.Errorf("%s: code processing finishes with error: %s", pipelineId, err.Error())
		}
		processSuccess(ctx, data, pipelineId, cacheService, successCaseStatus)
	}
	return nil
}

// cancelCheck checks cancel flag for code processing.
// If cancel flag doesn't exist in cache continue working.
// If context is done it means that code processing was finished (successfully/with error/timeout). Return.
// If cancel flag exists, and it is true it means that code processing was canceled. Set true to cancelChannel and return.
func cancelCheck(ctx context.Context, pipelineId uuid.UUID, cancelChannel chan bool, cacheService cache.Cache) {
	ticker := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			cancel, err := cacheService.GetValue(ctx, pipelineId, cache.Canceled)
			if err != nil {
				continue
			}
			if cancel.(bool) {
				cancelChannel <- true
			}
			return
		}
	}
}

// DeleteFolders removes all prepared folders for received LifeCycle
func DeleteFolders(pipelineId uuid.UUID, lc *fs_tool.LifeCycle) {
	logger.Infof("%s: DeleteFolders() ...\n", pipelineId)
	if err := lc.DeleteFolders(); err != nil {
		logger.Error("%s: DeleteFolders(): %s\n", pipelineId, err.Error())
	}
	logger.Infof("%s: DeleteFolders() complete\n", pipelineId)
	logger.Infof("%s: complete\n", pipelineId)
}

// finishByTimeout is used in case of runCode method finished by timeout
func finishByTimeout(ctx context.Context, pipelineId uuid.UUID, cacheService cache.Cache) {
	logger.Errorf("%s: code processing finishes because of timeout\n", pipelineId)

	// set to cache pipelineId: cache.SubKey_Status: Status_STATUS_RUN_TIMEOUT
	cacheService.SetValue(ctx, pipelineId, cache.Status, pb.Status_STATUS_RUN_TIMEOUT)
}

// processError processes error received during processing code via setting a corresponding status and output to cache
func processError(ctx context.Context, err error, data []byte, pipelineId uuid.UUID, cacheService cache.Cache, status pb.Status) {
	switch status {
	case pb.Status_STATUS_VALIDATION_ERROR:
		logger.Errorf("%s: Validate: %s\n", pipelineId, err.Error())

		cacheService.SetValue(ctx, pipelineId, cache.Status, pb.Status_STATUS_VALIDATION_ERROR)
	case pb.Status_STATUS_PREPARATION_ERROR:
		logger.Errorf("%s: Prepare: %s\n", pipelineId, err.Error())

		cacheService.SetValue(ctx, pipelineId, cache.Status, pb.Status_STATUS_PREPARATION_ERROR)
	case pb.Status_STATUS_COMPILE_ERROR:
		logger.Errorf("%s: Compile: err: %s, output: %s\n", pipelineId, err.Error(), data)

		cacheService.SetValue(ctx, pipelineId, cache.CompileOutput, "error: "+err.Error()+", output: "+string(data))

		cacheService.SetValue(ctx, pipelineId, cache.Status, pb.Status_STATUS_COMPILE_ERROR)
	case pb.Status_STATUS_RUN_ERROR:
		logger.Errorf("%s: Run: err: %s, output: %s\n", pipelineId, err.Error(), data)

		cacheService.SetValue(ctx, pipelineId, cache.RunError, "error: "+err.Error()+", output: "+string(data))

		cacheService.SetValue(ctx, pipelineId, cache.Status, pb.Status_STATUS_RUN_ERROR)
	}
}

// processSuccess processes case after successful code processing via setting a corresponding status and output to cache
func processSuccess(ctx context.Context, output []byte, pipelineId uuid.UUID, cacheService cache.Cache, status pb.Status) {
	switch status {
	case pb.Status_STATUS_PREPARING:
		logger.Infof("%s: Validate() finish\n", pipelineId)

		cacheService.SetValue(ctx, pipelineId, cache.Status, pb.Status_STATUS_PREPARING)
	case pb.Status_STATUS_COMPILING:
		logger.Infof("%s: Prepare() finish\n", pipelineId)

		cacheService.SetValue(ctx, pipelineId, cache.Status, pb.Status_STATUS_COMPILING)
	case pb.Status_STATUS_EXECUTING:
		logger.Infof("%s: Compile() finish\n", pipelineId)

		cacheService.SetValue(ctx, pipelineId, cache.CompileOutput, string(output))

		cacheService.SetValue(ctx, pipelineId, cache.RunOutput, "")

		cacheService.SetValue(ctx, pipelineId, cache.Status, pb.Status_STATUS_EXECUTING)
	case pb.Status_STATUS_FINISHED:
		logger.Infof("%s: Run() finish\n", pipelineId)

		cacheService.SetValue(ctx, pipelineId, cache.Status, pb.Status_STATUS_FINISHED)
	}
}

// processCancel process case when code processing was canceled
func processCancel(ctx context.Context, cacheService cache.Cache, pipelineId uuid.UUID) {
	logger.Infof("%s: was canceled\n", pipelineId)

	// set to cache pipelineId: cache.SubKey_Status: pb.Status_STATUS_CANCELED
	cacheService.SetValue(ctx, pipelineId, cache.Status, pb.Status_STATUS_CANCELED)
}
