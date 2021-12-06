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
	"beam.apache.org/playground/backend/internal/setup_tools/builder"
	"beam.apache.org/playground/backend/internal/streaming"
	"beam.apache.org/playground/backend/internal/utils"
	"beam.apache.org/playground/backend/internal/validators"
	"bytes"
	"context"
	"fmt"
	"github.com/google/uuid"
	"io"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"time"
)

const (
	pauseDuration = 500 * time.Millisecond
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
func Process(ctx context.Context, cacheService cache.Cache, lc *fs_tool.LifeCycle, pipelineId uuid.UUID, appEnv *environment.ApplicationEnvs, sdkEnv *environment.BeamEnvs) {
	ctxWithTimeout, finishCtxFunc := context.WithTimeout(ctx, appEnv.PipelineExecuteTimeout())
	defer func(lc *fs_tool.LifeCycle) {
		finishCtxFunc()
		DeleteFolders(pipelineId, lc)
	}(lc)

	errorChannel := make(chan error, 1)
	successChannel := make(chan bool, 1)
	cancelChannel := make(chan bool, 1)
	stopReadLogsChannel := make(chan bool, 1)
	finishReadLogsChannel := make(chan bool, 1)
	var validationResults sync.Map

	go cancelCheck(ctxWithTimeout, pipelineId, cancelChannel, cacheService)

	executorBuilder, err := builder.SetupExecutorBuilder(lc.GetAbsoluteSourceFilePath(), lc.GetAbsoluteBaseFolderPath(), lc.GetAbsoluteExecutableFilePath(), sdkEnv)
	if err != nil {
		_ = processSetupError(err, pipelineId, cacheService, ctxWithTimeout)
		return
	}
	executor := executorBuilder.Build()
	// Validate
	logger.Infof("%s: Validate() ...\n", pipelineId)
	validateFunc := executor.Validate()
	go validateFunc(successChannel, errorChannel, &validationResults)

	ok, err := processStep(ctxWithTimeout, pipelineId, cacheService, cancelChannel, successChannel)
	if err != nil {
		return
	}
	if !ok {
		_ = processError(ctxWithTimeout, errorChannel, pipelineId, cacheService, "Validate", pb.Status_STATUS_VALIDATION_ERROR)
		return
	}
	if err := processSuccess(ctxWithTimeout, pipelineId, cacheService, "Validate", pb.Status_STATUS_PREPARING); err != nil {
		return
	}

	// Prepare
	logger.Infof("%s: Prepare() ...\n", pipelineId)
	prepareFunc := executor.Prepare()
	go prepareFunc(successChannel, errorChannel)

	ok, err = processStep(ctxWithTimeout, pipelineId, cacheService, cancelChannel, successChannel)
	if err != nil {
		return
	}
	if !ok {
		_ = processError(ctxWithTimeout, errorChannel, pipelineId, cacheService, "Prepare", pb.Status_STATUS_PREPARATION_ERROR)
		return
	}
	if err := processSuccess(ctxWithTimeout, pipelineId, cacheService, "Prepare", pb.Status_STATUS_COMPILING); err != nil {
		return
	}

	switch sdkEnv.ApacheBeamSdk {
	case pb.Sdk_SDK_JAVA, pb.Sdk_SDK_GO:
		// Compile
		logger.Infof("%s: Compile() ...\n", pipelineId)
		compileCmd := executor.Compile(ctxWithTimeout)
		var compileError bytes.Buffer
		var compileOutput bytes.Buffer
		runCmdWithOutput(compileCmd, &compileOutput, &compileError, successChannel, errorChannel)

		ok, err = processStep(ctxWithTimeout, pipelineId, cacheService, cancelChannel, successChannel)
		if err != nil {
			return
		}
		if !ok {
			_ = processCompileError(ctxWithTimeout, errorChannel, compileError.Bytes(), pipelineId, cacheService)
			return
		}
		if err := processCompileSuccess(ctxWithTimeout, compileOutput.Bytes(), pipelineId, cacheService); err != nil {
			return
		}
	case pb.Sdk_SDK_PYTHON:
		if err := processCompileSuccess(ctxWithTimeout, []byte(""), pipelineId, cacheService); err != nil {
			return
		}
	}

	// Run
	if sdkEnv.ApacheBeamSdk == pb.Sdk_SDK_JAVA {
		executor, err = setJavaExecutableFile(lc, pipelineId, cacheService, ctxWithTimeout, executorBuilder, appEnv.WorkingDir())
		if err != nil {
			return
		}
	}
	runCmd := getExecuteCmd(&validationResults, &executor, ctxWithTimeout)
	var runError bytes.Buffer
	runOutput := streaming.RunOutputWriter{Ctx: ctxWithTimeout, CacheService: cacheService, PipelineId: pipelineId}
	go readLogFile(ctxWithTimeout, cacheService, lc.GetAbsoluteLogFilePath(), pipelineId, stopReadLogsChannel, finishReadLogsChannel)
	runCmdWithOutput(runCmd, &runOutput, &runError, successChannel, errorChannel)

	ok, err = processStep(ctxWithTimeout, pipelineId, cacheService, cancelChannel, successChannel)
	if err != nil {
		return
	}
	if !ok {
		_ = processRunError(ctxWithTimeout, errorChannel, runError.Bytes(), pipelineId, cacheService, stopReadLogsChannel, finishReadLogsChannel)
		return
	}
	_ = processRunSuccess(ctxWithTimeout, pipelineId, cacheService, stopReadLogsChannel, finishReadLogsChannel)
}

// getExecuteCmd return cmd instance based on the code type: unit test or example code
func getExecuteCmd(valRes *sync.Map, executor *executors.Executor, ctxWithTimeout context.Context) *exec.Cmd {
	isUnitTest, _ := valRes.Load(validators.UnitTestValidatorName)
	runType := executors.Run
	if isUnitTest.(bool) {
		runType = executors.Test
	}
	cmdReflect := reflect.ValueOf(executor).MethodByName(string(runType)).Call([]reflect.Value{reflect.ValueOf(ctxWithTimeout)})
	return cmdReflect[0].Interface().(*exec.Cmd)
}

// setJavaExecutableFile sets executable file name to runner (JAVA class name is known after compilation step)
func setJavaExecutableFile(lc *fs_tool.LifeCycle, id uuid.UUID, service cache.Cache, ctx context.Context, executorBuilder *executors.ExecutorBuilder, dir string) (executors.Executor, error) {
	className, err := lc.ExecutableName(id, dir)
	if err != nil {
		if err = processSetupError(err, id, service, ctx); err != nil {
			return executorBuilder.Build(), err
		}
	}
	return executorBuilder.
		WithExecutableFileName(className).
		Build(), nil
}

// processSetupError processes errors during the setting up an executor builder
func processSetupError(err error, pipelineId uuid.UUID, cacheService cache.Cache, ctxWithTimeout context.Context) error {
	logger.Errorf("%s: error during setup builder: %s\n", pipelineId, err.Error())
	if err = utils.SetToCache(ctxWithTimeout, cacheService, pipelineId, cache.Status, pb.Status_STATUS_ERROR); err != nil {
		return err
	}
	return nil
}

// GetProcessingOutput gets processing output value from cache by key and subKey.
// In case key doesn't exist in cache - returns an errors.NotFoundError.
// In case subKey doesn't exist in cache for the key - returns an errors.NotFoundError.
// In case value from cache by key and subKey couldn't be converted to string - returns an errors.InternalError.
func GetProcessingOutput(ctx context.Context, cacheService cache.Cache, key uuid.UUID, subKey cache.SubKey, errorTitle string) (string, error) {
	value, err := cacheService.GetValue(ctx, key, subKey)
	if err != nil {
		logger.Errorf("%s: GetStringValueFromCache(): cache.GetValue: error: %s", key, err.Error())
		return "", errors.NotFoundError(errorTitle, "Error during getting cache by key: %s, subKey: %s", key.String(), string(subKey))
	}
	stringValue, converted := value.(string)
	if !converted {
		logger.Errorf("%s: couldn't convert value to string: %s", key, value)
		return "", errors.InternalError(errorTitle, "Value from cache couldn't be converted to string: %s", value)
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
		return pb.Status_STATUS_UNSPECIFIED, errors.NotFoundError(errorTitle, "Error during getting cache by key: %s, subKey: %s", key.String(), string(cache.Status))
	}
	statusValue, converted := value.(pb.Status)
	if !converted {
		logger.Errorf("%s: couldn't convert value to correct status enum: %s", key, value)
		return pb.Status_STATUS_UNSPECIFIED, errors.InternalError(errorTitle, "Value from cache couldn't be converted to correct status enum: %s", value)
	}
	return statusValue, nil
}

// GetLastIndex gets last index for run output or logs from cache by key.
// In case key doesn't exist in cache - returns an errors.NotFoundError.
// In case value from cache by key and subKey couldn't be converted to int - returns an errors.InternalError.
func GetLastIndex(ctx context.Context, cacheService cache.Cache, key uuid.UUID, subKey cache.SubKey, errorTitle string) (int, error) {
	value, err := cacheService.GetValue(ctx, key, subKey)
	if err != nil {
		logger.Errorf("%s: GetLastIndex(): cache.GetValue: error: %s", key, err.Error())
		return 0, errors.NotFoundError(errorTitle, "Error during getting cache by key: %s, subKey: %s", key.String(), string(subKey))
	}
	intValue, converted := value.(int)
	if !converted {
		logger.Errorf("%s: couldn't convert value to int: %s", key, value)
		return 0, errors.InternalError(errorTitle, "Value from cache couldn't be converted to int: %s", value)
	}
	return intValue, nil
}

// runCmdWithOutput runs command with keeping stdOut and stdErr
func runCmdWithOutput(cmd *exec.Cmd, stdOutput io.Writer, stdError *bytes.Buffer, successChannel chan bool, errorChannel chan error) {
	cmd.Stdout = stdOutput
	cmd.Stderr = stdError
	go func(cmd *exec.Cmd, successChannel chan bool, errChannel chan error) {
		err := cmd.Run()
		if err != nil {
			errChannel <- err
			successChannel <- false
		} else {
			successChannel <- true
		}
	}(cmd, successChannel, errorChannel)
}

// processStep processes each executor's step with cancel and timeout checks.
// If finishes by canceling, timeout or error - returns error.
// If finishes successfully with no error during step processing - returns true.
// If finishes successfully but with some error during step processing - returns false.
func processStep(ctx context.Context, pipelineId uuid.UUID, cacheService cache.Cache, cancelChannel, successChannel chan bool) (bool, error) {
	select {
	case <-ctx.Done():
		_ = finishByTimeout(ctx, pipelineId, cacheService)
		return false, fmt.Errorf("%s: context was done", pipelineId)
	case <-cancelChannel:
		_ = processCancel(ctx, cacheService, pipelineId)
		return false, fmt.Errorf("%s: code processing was canceled", pipelineId)
	case ok := <-successChannel:
		return ok, nil
	}
}

// cancelCheck checks cancel flag for code processing.
// If cancel flag doesn't exist in cache continue working.
// If context is done it means that the code processing was finished (successfully/with error/timeout). Return.
// If cancel flag exists, and it is true it means that the code processing was canceled. Set true to cancelChannel and return.
func cancelCheck(ctx context.Context, pipelineId uuid.UUID, cancelChannel chan bool, cacheService cache.Cache) {
	ticker := time.NewTicker(pauseDuration)
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

// readLogFile reads logs from the log file and keeps it to the cache.
// If context is done it means that the code processing was finished (successfully/with error/timeout). Write last logs to the cache.
// If <-stopReadLogsChannel it means that the code processing was finished (canceled/timeout)
// 	and it waits until the method stops the work to change status to the pb.Status_STATUS_FINISHED. Write last logs
//	to the cache and set value to the finishReadLogChannel channel to unblock the code processing.
// In other case each pauseDuration write to cache logs of the code processing.
func readLogFile(ctx context.Context, cacheService cache.Cache, logFilePath string, pipelineId uuid.UUID, stopReadLogsChannel, finishReadLogChannel chan bool) {
	ticker := time.NewTicker(pauseDuration)
	for {
		select {
		// in case of timeout or cancel
		case <-ctx.Done():
			_ = finishReadLogFile(ctx, ticker, cacheService, logFilePath, pipelineId)
			return
		// in case of pipeline finish successfully or has error on the run step
		case <-stopReadLogsChannel:
			_ = finishReadLogFile(ctx, ticker, cacheService, logFilePath, pipelineId)
			finishReadLogChannel <- true
			return
		case <-ticker.C:
			_ = writeLogsToCache(ctx, cacheService, logFilePath, pipelineId)
		}
	}
}

// finishReadLogFile is used to read logs file for the last time
func finishReadLogFile(ctx context.Context, ticker *time.Ticker, cacheService cache.Cache, logFilePath string, pipelineId uuid.UUID) error {
	ticker.Stop()
	return writeLogsToCache(ctx, cacheService, logFilePath, pipelineId)
}

// writeLogsToCache write all logs from the log file to the cache.
// If log file doesn't exist, return nil.
//	Reading logs works as a parallel with code processing so when program tries to read file
//	it could be that the file doesn't exist yet.
// If log file exists, read all from the log file and keep it to the cache using cache.Logs subKey.
// If some error occurs, log the error and return the error.
func writeLogsToCache(ctx context.Context, cacheService cache.Cache, logFilePath string, pipelineId uuid.UUID) error {
	if _, err := os.Stat(logFilePath); os.IsNotExist(err) {
		return nil
	}
	logs, err := os.ReadFile(logFilePath)
	if err != nil {
		logger.Errorf("%s: writeLogsToCache(): error during read from logs file: %s", pipelineId, err.Error())
		return err
	}
	return utils.SetToCache(ctx, cacheService, pipelineId, cache.Logs, string(logs))
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
func finishByTimeout(ctx context.Context, pipelineId uuid.UUID, cacheService cache.Cache) error {
	logger.Errorf("%s: code processing finishes because of timeout\n", pipelineId)

	// set to cache pipelineId: cache.SubKey_Status: Status_STATUS_RUN_TIMEOUT
	return utils.SetToCache(ctx, cacheService, pipelineId, cache.Status, pb.Status_STATUS_RUN_TIMEOUT)
}

// processError processes error received during processing validation or preparation steps.
// This method sets corresponding status to the cache.
func processError(ctx context.Context, errorChannel chan error, pipelineId uuid.UUID, cacheService cache.Cache, errorTitle string, newStatus pb.Status) error {
	err := <-errorChannel
	logger.Errorf("%s: %s(): %s\n", pipelineId, errorTitle, err.Error())

	return utils.SetToCache(ctx, cacheService, pipelineId, cache.Status, newStatus)
}

// processCompileError processes error received during processing compile step.
// This method sets error output and corresponding status to the cache.
func processCompileError(ctx context.Context, errorChannel chan error, errorOutput []byte, pipelineId uuid.UUID, cacheService cache.Cache) error {
	err := <-errorChannel
	logger.Errorf("%s: Compile(): err: %s, output: %s\n", pipelineId, err.Error(), errorOutput)

	if err := utils.SetToCache(ctx, cacheService, pipelineId, cache.CompileOutput, "error: "+err.Error()+", output: "+string(errorOutput)); err != nil {
		return err
	}
	return utils.SetToCache(ctx, cacheService, pipelineId, cache.Status, pb.Status_STATUS_COMPILE_ERROR)
}

// processRunError processes error received during processing run step.
// This method sets error output to the cache and after that sets value to channel to stop goroutine which writes logs.
//	After receiving a signal that goroutine was finished (read value from finishReadLogsChannel) this method
//	sets corresponding status to the cache.
func processRunError(ctx context.Context, errorChannel chan error, errorOutput []byte, pipelineId uuid.UUID, cacheService cache.Cache, stopReadLogsChannel, finishReadLogsChannel chan bool) error {
	err := <-errorChannel
	logger.Errorf("%s: Run(): err: %s, output: %s\n", pipelineId, err.Error(), errorOutput)

	if err := utils.SetToCache(ctx, cacheService, pipelineId, cache.RunError, "error: "+err.Error()+", output: "+string(errorOutput)); err != nil {
		return err
	}

	stopReadLogsChannel <- true
	<-finishReadLogsChannel

	return utils.SetToCache(ctx, cacheService, pipelineId, cache.Status, pb.Status_STATUS_RUN_ERROR)
}

// processSuccess processes case after successful process validation or preparation steps.
// This method sets corresponding status to the cache.
func processSuccess(ctx context.Context, pipelineId uuid.UUID, cacheService cache.Cache, successTitle string, newStatus pb.Status) error {
	logger.Infof("%s: %s(): finish\n", pipelineId, successTitle)

	return utils.SetToCache(ctx, cacheService, pipelineId, cache.Status, newStatus)
}

// processCompileSuccess processes case after successful compile step.
// This method sets output of the compile step, sets empty string as output of the run step and
//	sets corresponding status to the cache.
func processCompileSuccess(ctx context.Context, output []byte, pipelineId uuid.UUID, cacheService cache.Cache) error {
	logger.Infof("%s: Compile() finish\n", pipelineId)

	if err := utils.SetToCache(ctx, cacheService, pipelineId, cache.CompileOutput, string(output)); err != nil {
		return err
	}
	if err := utils.SetToCache(ctx, cacheService, pipelineId, cache.RunOutput, ""); err != nil {
		return err
	}
	return utils.SetToCache(ctx, cacheService, pipelineId, cache.Status, pb.Status_STATUS_EXECUTING)
}

// processRunSuccess processes case after successful run step.
// This method sets value to channel to stop goroutine which writes logs.
//	After receiving a signal that goroutine was finished (read value from finishReadLogsChannel) this method
//	sets corresponding status to the cache.
func processRunSuccess(ctx context.Context, pipelineId uuid.UUID, cacheService cache.Cache, stopReadLogsChannel, finishReadLogsChannel chan bool) error {
	logger.Infof("%s: Run() finish\n", pipelineId)

	stopReadLogsChannel <- true
	<-finishReadLogsChannel

	return utils.SetToCache(ctx, cacheService, pipelineId, cache.Status, pb.Status_STATUS_FINISHED)
}

// processCancel process case when code processing was canceled
func processCancel(ctx context.Context, cacheService cache.Cache, pipelineId uuid.UUID) error {
	logger.Infof("%s: was canceled\n", pipelineId)

	// set to cache pipelineId: cache.SubKey_Status: pb.Status_STATUS_CANCELED
	return utils.SetToCache(ctx, cacheService, pipelineId, cache.Status, pb.Status_STATUS_CANCELED)
}
