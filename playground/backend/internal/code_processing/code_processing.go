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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"

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
func Process(ctx context.Context, cacheService cache.Cache, lc *fs_tool.LifeCycle, pipelineId uuid.UUID, appEnv *environment.ApplicationEnvs, sdkEnv *environment.BeamEnvs, pipelineOptions string) {
	pipelineLifeCycleCtx, finishCtxFunc := context.WithTimeout(ctx, appEnv.PipelineExecuteTimeout())
	defer func(lc *fs_tool.LifeCycle) {
		finishCtxFunc()
		DeleteFolders(pipelineId, lc)
	}(lc)

	cancelChannel := make(chan bool, 1)

	var validationResults sync.Map

	go cancelCheck(pipelineLifeCycleCtx, pipelineId, cancelChannel, cacheService)

	executor := validateStep(ctx, cacheService, &lc.Paths, pipelineId, sdkEnv, pipelineLifeCycleCtx, &validationResults, cancelChannel)
	if executor == nil {
		return
	}

	executor = prepareStep(ctx, cacheService, &lc.Paths, pipelineId, sdkEnv, pipelineLifeCycleCtx, &validationResults, cancelChannel)
	if executor == nil {
		return
	}

	// Check if is unit test
	validateIsUnitTest, _ := validationResults.Load(validators.UnitTestValidatorName)
	isUnitTest := validateIsUnitTest.(bool)

	executor = compileStep(ctx, cacheService, &lc.Paths, pipelineId, sdkEnv, isUnitTest, pipelineLifeCycleCtx, cancelChannel)
	if executor == nil {
		return
	}

	// Run/RunTest
	runStep(ctx, cacheService, &lc.Paths, pipelineId, isUnitTest, sdkEnv, pipelineOptions, pipelineLifeCycleCtx, cancelChannel)
}

func runStep(ctx context.Context, cacheService cache.Cache, paths *fs_tool.LifeCyclePaths, pipelineId uuid.UUID, isUnitTest bool, sdkEnv *environment.BeamEnvs, pipelineOptions string, pipelineLifeCycleCtx context.Context, cancelChannel chan bool) {
	errorChannel, successChannel := createStatusChannels()
	stopReadLogsChannel := make(chan bool, 1)
	finishReadLogsChannel := make(chan bool, 1)

	var executorBuilder *executors.ExecutorBuilder
	err := error(nil)
	if isUnitTest {
		executorBuilder, err = builder.TestRunner(paths, sdkEnv)
	} else {
		executorBuilder, err = builder.Runner(paths, utils.ReduceWhiteSpacesToSinge(pipelineOptions), sdkEnv)
	}
	if err != nil {
		_ = processSetupError(err, pipelineId, cacheService, pipelineLifeCycleCtx)
		return
	}

	executor := executorBuilder.Build()
	logger.Infof("%s: Run()/Test() ...\n", pipelineId)
	runCmd := getExecuteCmd(isUnitTest, &executor, pipelineLifeCycleCtx)
	var runError bytes.Buffer
	runOutput := streaming.RunOutputWriter{Ctx: pipelineLifeCycleCtx, CacheService: cacheService, PipelineId: pipelineId}
	go readLogFile(pipelineLifeCycleCtx, ctx, cacheService, paths.AbsoluteLogFilePath, pipelineId, stopReadLogsChannel, finishReadLogsChannel)
	go readGraphFile(pipelineLifeCycleCtx, ctx, cacheService, paths.AbsoluteGraphFilePath, pipelineId)

	if sdkEnv.ApacheBeamSdk == pb.Sdk_SDK_GO {
		// For go SDK all logs are placed to stdErr.
		file, err := os.Create(paths.AbsoluteLogFilePath)
		if err != nil {
			// If some error with creating a log file do the same as with other SDK.
			logger.Errorf("%s: error during create log file (go sdk): %s", pipelineId, err.Error())
			runCmdWithOutput(runCmd, &runOutput, &runError, successChannel, errorChannel)
		} else {
			// Use the log file to write all stdErr into it.
			runCmdWithOutput(runCmd, &runOutput, file, successChannel, errorChannel)
		}
	} else {
		// Other SDKs write logs to the log file on their own.
		runCmdWithOutput(runCmd, &runOutput, &runError, successChannel, errorChannel)
	}

	// Start of the monitoring of background tasks (run step/cancellation/timeout)
	ok, err := reconcileBackgroundTask(pipelineLifeCycleCtx, ctx, pipelineId, cacheService, cancelChannel, successChannel)
	if err != nil {
		return
	}
	if !ok {
		// If unit test has some error then error output is placed as RunOutput
		if isUnitTest {
			output, err := GetProcessingOutput(ctx, cacheService, pipelineId, cache.RunOutput, "")
			if err == nil {
				runError.Write([]byte(output))
			}

		}
		// Run step is finished, but code contains some error (divide by 0 for example)
		if sdkEnv.ApacheBeamSdk == pb.Sdk_SDK_GO {
			// For Go SDK stdErr was redirected to the log file.
			errData, err := os.ReadFile(paths.AbsoluteLogFilePath)
			if err != nil {
				logger.Errorf("%s: error during read errors from log file (go sdk): %s", pipelineId, err.Error())
			}
			runError.Write(errData)
		}
		_ = processRunError(pipelineLifeCycleCtx, errorChannel, runError.Bytes(), pipelineId, cacheService, stopReadLogsChannel, finishReadLogsChannel)
		return
	}
	// Run step is finished and code is executed
	_ = processRunSuccess(pipelineLifeCycleCtx, pipelineId, cacheService, stopReadLogsChannel, finishReadLogsChannel)
}

func compileStep(ctx context.Context, cacheService cache.Cache, paths *fs_tool.LifeCyclePaths, pipelineId uuid.UUID, sdkEnv *environment.BeamEnvs, isUnitTest bool, pipelineLifeCycleCtx context.Context, cancelChannel chan bool) *executors.Executor {
	errorChannel, successChannel := createStatusChannels()
	var executor = executors.Executor{}
	// This condition is used for cases when the playground doesn't compile source files. For the Python code and the Go Unit Tests
	if sdkEnv.ApacheBeamSdk == pb.Sdk_SDK_PYTHON || sdkEnv.ApacheBeamSdk == pb.Sdk_SDK_SCIO || (sdkEnv.ApacheBeamSdk == pb.Sdk_SDK_GO && isUnitTest) {
		if err := processCompileSuccess(pipelineLifeCycleCtx, []byte(""), pipelineId, cacheService); err != nil {
			return nil
		}
	} else { // in case of Java, Go (not unit test), Scala - need compile step
		executorBuilder := builder.Compiler(paths, sdkEnv)
		executor := executorBuilder.Build()
		logger.Infof("%s: Compile() ...\n", pipelineId)
		compileCmd := executor.Compile(pipelineLifeCycleCtx)
		var compileError bytes.Buffer
		var compileOutput bytes.Buffer
		runCmdWithOutput(compileCmd, &compileOutput, &compileError, successChannel, errorChannel)

		// Start of the monitoring of background tasks (compile step/cancellation/timeout)
		ok, err := reconcileBackgroundTask(pipelineLifeCycleCtx, ctx, pipelineId, cacheService, cancelChannel, successChannel)
		if err != nil {
			return nil
		}
		if !ok { // Compile step is finished, but code couldn't be compiled (some typos for example)
			err := <-errorChannel
			_ = processErrorWithSavingOutput(pipelineLifeCycleCtx, err, compileError.Bytes(), pipelineId, cache.CompileOutput, cacheService, "Compile", pb.Status_STATUS_COMPILE_ERROR)
			return nil
		} // Compile step is finished and code is compiled
		if err := processCompileSuccess(pipelineLifeCycleCtx, compileOutput.Bytes(), pipelineId, cacheService); err != nil {
			return nil
		}
	}
	return &executor
}

func prepareStep(ctx context.Context, cacheService cache.Cache, paths *fs_tool.LifeCyclePaths, pipelineId uuid.UUID, sdkEnv *environment.BeamEnvs, pipelineLifeCycleCtx context.Context, validationResults *sync.Map, cancelChannel chan bool) *executors.Executor {
	errorChannel, successChannel := createStatusChannels()
	executorBuilder, err := builder.Preparer(paths, sdkEnv, validationResults)
	if err != nil {
		_ = processSetupError(err, pipelineId, cacheService, pipelineLifeCycleCtx)
		return nil
	}
	executor := executorBuilder.Build()
	logger.Infof("%s: Prepare() ...\n", pipelineId)
	prepareFunc := executor.Prepare()
	go prepareFunc(successChannel, errorChannel, validationResults)

	// Start of the monitoring of background tasks (prepare function/cancellation/timeout)
	ok, err := reconcileBackgroundTask(pipelineLifeCycleCtx, ctx, pipelineId, cacheService, cancelChannel, successChannel)
	if err != nil {
		return nil
	}
	if !ok {
		err := <-errorChannel
		// Prepare step is finished, but code couldn't be prepared (some error during prepare step)
		_ = processErrorWithSavingOutput(pipelineLifeCycleCtx, err, []byte(err.Error()), pipelineId, cache.PreparationOutput, cacheService, "Prepare", pb.Status_STATUS_PREPARATION_ERROR)
		return nil
	}
	// Prepare step is finished and code is prepared
	if err := processSuccess(pipelineLifeCycleCtx, pipelineId, cacheService, "Prepare", pb.Status_STATUS_COMPILING); err != nil {
		return nil
	}
	return &executor
}

func validateStep(ctx context.Context, cacheService cache.Cache, paths *fs_tool.LifeCyclePaths, pipelineId uuid.UUID, sdkEnv *environment.BeamEnvs, pipelineLifeCycleCtx context.Context, validationResults *sync.Map, cancelChannel chan bool) *executors.Executor {
	errorChannel, successChannel := createStatusChannels()
	executorBuilder, err := builder.Validator(paths, sdkEnv)
	if err != nil {
		_ = processSetupError(err, pipelineId, cacheService, pipelineLifeCycleCtx)
		return nil
	}
	executor := executorBuilder.Build()
	logger.Infof("%s: Validate() ...\n", pipelineId)
	validateFunc := executor.Validate()
	go validateFunc(successChannel, errorChannel, validationResults)

	// Start of the monitoring of background tasks (validate function/cancellation/timeout)
	ok, err := reconcileBackgroundTask(pipelineLifeCycleCtx, ctx, pipelineId, cacheService, cancelChannel, successChannel)
	if err != nil {
		return nil
	}
	if !ok {
		err := <-errorChannel
		// Validate step is finished, but code isn't valid
		_ = processErrorWithSavingOutput(pipelineLifeCycleCtx, err, []byte(err.Error()), pipelineId, cache.ValidationOutput, cacheService, "Validate", pb.Status_STATUS_VALIDATION_ERROR)
		return nil
	}

	// Validate step is finished and code is valid
	if err := processSuccess(pipelineLifeCycleCtx, pipelineId, cacheService, "Validate", pb.Status_STATUS_PREPARING); err != nil {
		return nil
	}
	return &executor
}

func createStatusChannels() (chan error, chan bool) {
	errorChannel := make(chan error, 1)
	successChannel := make(chan bool, 1)
	return errorChannel, successChannel
}

// getExecuteCmd return cmd instance based on the code type: unit test or example code
func getExecuteCmd(isUnitTest bool, executor *executors.Executor, ctxWithTimeout context.Context) *exec.Cmd {
	runType := executors.Run
	if isUnitTest {
		runType = executors.Test
	}
	cmdReflect := reflect.ValueOf(executor).MethodByName(string(runType)).Call([]reflect.Value{reflect.ValueOf(ctxWithTimeout)})
	return cmdReflect[0].Interface().(*exec.Cmd)
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
		logger.Errorf("%s: GetProcessingOutput(): cache.GetValue: error: %s", key, err.Error())
		return "", errors.NotFoundError(errorTitle, "Error during getting output")
	}
	stringValue, converted := value.(string)
	if !converted {
		logger.Errorf("%s: couldn't convert value to string: %s", key, value)
		return "", errors.InternalError(errorTitle, "Error during getting output")
	}
	return stringValue, nil
}

// GetProcessingStatus gets processing status from cache by key.
// In case key doesn't exist in cache - returns an errors.NotFoundError.
// In case value from cache by key and subKey couldn't be converted to playground.Status - returns an errors.InternalError.
func GetProcessingStatus(ctx context.Context, cacheService cache.Cache, key uuid.UUID, errorTitle string) (pb.Status, error) {
	value, err := cacheService.GetValue(ctx, key, cache.Status)
	if err != nil {
		logger.Errorf("%s: GetProcessingStatus(): cache.GetValue: error: %s", key, err.Error())
		return pb.Status_STATUS_UNSPECIFIED, errors.NotFoundError(errorTitle, "Error during getting status")
	}
	statusValue, converted := value.(pb.Status)
	if !converted {
		logger.Errorf("%s: couldn't convert value to correct status enum: %s", key, value)
		return pb.Status_STATUS_UNSPECIFIED, errors.InternalError(errorTitle, "Error during getting status")
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
		return 0, errors.NotFoundError(errorTitle, "Error during getting pagination value")
	}
	convertedValue, converted := value.(float64)
	if !converted {
		logger.Errorf("%s: couldn't convert value to float64. value: %s type %s", key, value, reflect.TypeOf(value))
		return 0, errors.InternalError(errorTitle, "Error during getting pagination value")
	}
	return int(convertedValue), nil
}

// GetGraph gets graph from cache by key.
// In case key doesn't exist in cache - returns an errors.NotFoundError.
// In case value from cache by key couldn't be converted to []byte - returns an errors.InternalError.
func GetGraph(ctx context.Context, cacheService cache.Cache, key uuid.UUID, errorTitle string) (string, error) {
	value, err := cacheService.GetValue(ctx, key, cache.Graph)
	if err != nil {
		logger.Errorf("%s: GetGraph(): cache.GetValue: error: %s", key, err.Error())
		return "", errors.NotFoundError(errorTitle, "Error during getting graph")
	}
	stringValue, converted := value.(string)
	if !converted {
		logger.Errorf("%s: couldn't convert value to string. value: %s type %s", key, value, reflect.TypeOf(value))
		return "", errors.InternalError(errorTitle, "Error during getting graph")
	}
	return stringValue, nil
}

// runCmdWithOutput runs command with keeping stdOut and stdErr
func runCmdWithOutput(cmd *exec.Cmd, stdOutput io.Writer, stdError io.Writer, successChannel chan bool, errorChannel chan error) {
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

// reconcileBackgroundTask waits when first background task finishes.
// If finishes by canceling, timeout or context is done - returns error.
// If cmd operation (Validate/Prepare/Compile/Run/RunTest) finishes successfully with no error
//  during step processing - returns true.
// If cmd operation (Validate/Prepare/Compile/Run/RunTest) finishes successfully but with some error
//  during step processing - returns false.
func reconcileBackgroundTask(pipelineLifeCycleCtx, backgroundCtx context.Context, pipelineId uuid.UUID, cacheService cache.Cache, cancelChannel, successChannel chan bool) (bool, error) {
	select {
	case <-pipelineLifeCycleCtx.Done():
		_ = finishByTimeout(backgroundCtx, pipelineId, cacheService)
		return false, fmt.Errorf("%s: context was done", pipelineId)
	case <-cancelChannel:
		_ = processCancel(pipelineLifeCycleCtx, cacheService, pipelineId)
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
				logger.Errorf("%s: Error during getting value from the cache: %s", pipelineId, err.Error())
			}
			if cancel.(bool) {
				cancelChannel <- true
				return
			}
		}
	}
}

// readGraphFile reads graph from the file and keeps it to the cache.
// If context is done it means that the code processing was finished (successfully/with error/timeout).
// Write graph to the cache if this in the file.
// In other case each pauseDuration checks that graph file exists or not and try to save it to the cache.
func readGraphFile(pipelineLifeCycleCtx, backgroundCtx context.Context, cacheService cache.Cache, graphFilePath string, pipelineId uuid.UUID) {
	ticker := time.NewTicker(pauseDuration)
	for {
		select {
		// waiting when graph file appears
		case <-ticker.C:
			_, err := os.Stat(graphFilePath)
			if err == nil {
				ticker.Stop()
				graph, err := utils.ReadFile(pipelineId, graphFilePath)
				if err != nil {
					logger.Errorf("%s: Error during saving graph to the file: %s", pipelineId, err.Error())
				}
				_ = utils.SetToCache(backgroundCtx, cacheService, pipelineId, cache.Graph, graph)
			}
		// in case of timeout or cancel
		case <-pipelineLifeCycleCtx.Done():
			ticker.Stop()
			if _, err := os.Stat(graphFilePath); err == nil {
				graph, err := utils.ReadFile(pipelineId, graphFilePath)
				if err != nil {
					logger.Errorf("%s: Error during saving graph to the file: %s", pipelineId, err.Error())
				}
				_ = utils.SetToCache(backgroundCtx, cacheService, pipelineId, cache.Graph, graph)
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
func readLogFile(pipelineLifeCycleCtx, backgroundCtx context.Context, cacheService cache.Cache, logFilePath string, pipelineId uuid.UUID, stopReadLogsChannel, finishReadLogChannel chan bool) {
	ticker := time.NewTicker(pauseDuration)
	for {
		select {
		// in case of timeout or cancel
		case <-pipelineLifeCycleCtx.Done():
			_ = finishReadLogFile(backgroundCtx, ticker, cacheService, logFilePath, pipelineId)
			return
		// in case of pipeline finish successfully or has error on the run step
		case <-stopReadLogsChannel:
			_ = finishReadLogFile(pipelineLifeCycleCtx, ticker, cacheService, logFilePath, pipelineId)
			finishReadLogChannel <- true
			return
		case <-ticker.C:
			_ = writeLogsToCache(pipelineLifeCycleCtx, cacheService, logFilePath, pipelineId)
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

// processErrorWithSavingOutput processes error with saving to cache received error output.
func processErrorWithSavingOutput(ctx context.Context, err error, errorOutput []byte, pipelineId uuid.UUID, subKey cache.SubKey, cacheService cache.Cache, errorTitle string, newStatus pb.Status) error {
	logger.Errorf("%s: %s(): err: %s, output: %s\n", pipelineId, errorTitle, err.Error(), errorOutput)

	if err := utils.SetToCache(ctx, cacheService, pipelineId, subKey, fmt.Sprintf("error: %s\noutput: %s", err.Error(), errorOutput)); err != nil {
		return err
	}

	return utils.SetToCache(ctx, cacheService, pipelineId, cache.Status, newStatus)
}

// processRunError processes error received during processing run step.
// This method sets error output to the cache and after that sets value to channel to stop goroutine which writes logs.
//	After receiving a signal that goroutine was finished (read value from finishReadLogsChannel) this method
//	sets corresponding status to the cache.
func processRunError(ctx context.Context, errorChannel chan error, errorOutput []byte, pipelineId uuid.UUID, cacheService cache.Cache, stopReadLogsChannel, finishReadLogsChannel chan bool) error {
	err := <-errorChannel
	logger.Errorf("%s: Run(): err: %s, output: %s\n", pipelineId, err.Error(), errorOutput)

	if err := utils.SetToCache(ctx, cacheService, pipelineId, cache.RunError, fmt.Sprintf("error: %s\noutput: %s", err.Error(), string(errorOutput))); err != nil {
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
	if err := utils.SetToCache(ctx, cacheService, pipelineId, cache.RunError, ""); err != nil {
		return err
	}
	if err := utils.SetToCache(ctx, cacheService, pipelineId, cache.Logs, ""); err != nil {
		return err
	}
	if err := utils.SetToCache(ctx, cacheService, pipelineId, cache.Graph, ""); err != nil {
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
