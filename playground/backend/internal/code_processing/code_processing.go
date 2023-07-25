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
	"errors"
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
	perrors "beam.apache.org/playground/backend/internal/errors"
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
		DeleteResources(pipelineId, lc)
	}(lc)

	var validationResults sync.Map

	go cancelCheck(pipelineLifeCycleCtx, pipelineId, finishCtxFunc, cacheService)

	err := validateStep(pipelineLifeCycleCtx, cacheService, &lc.Paths, pipelineId, sdkEnv, &validationResults)
	if err != nil {
		var pipelineCanceledError perrors.PipelineCanceledError
		if errors.As(err, &pipelineCanceledError) {
			logger.Warnf("%s: pipeline execution has been canceled: %s", pipelineId, pipelineCanceledError.Error())
		} else {
			logger.Errorf("%s: error during validation step: %s", pipelineId, err.Error())
		}
		return
	}

	err = prepareStep(pipelineLifeCycleCtx, cacheService, &lc.Paths, pipelineId, sdkEnv, &validationResults, lc.GetPreparerParameters())
	if err != nil {
		var pipelineCanceledError perrors.PipelineCanceledError
		if errors.As(err, &pipelineCanceledError) {
			logger.Warnf("%s: pipeline execution has been canceled: %s", pipelineId, pipelineCanceledError.Error())
		} else {
			logger.Errorf("%s: error during preparation step: %s", pipelineId, err.Error())
		}
		return
	}

	// Check if is unit test
	validateIsUnitTest, _ := validationResults.Load(validators.UnitTestValidatorName)
	isUnitTest := validateIsUnitTest.(bool)

	err = compileStep(pipelineLifeCycleCtx, cacheService, &lc.Paths, pipelineId, sdkEnv, isUnitTest)
	if err != nil {
		var pipelineCanceledError perrors.PipelineCanceledError
		var compilationError perrors.CompilationError
		if errors.As(err, &pipelineCanceledError) {
			logger.Warnf("%s: pipeline execution has been canceled: %s", pipelineId, pipelineCanceledError.Error())
		} else if errors.As(err, &compilationError) {
			logger.Warnf("%s: compilation error: %s", pipelineId, compilationError.Error())
		} else {
			logger.Errorf("%s: error during compilation step: %s", pipelineId, err.Error())
		}
		return
	}

	// Run/RunTest
	err = runStep(pipelineLifeCycleCtx, cacheService, &lc.Paths, pipelineId, isUnitTest, sdkEnv, pipelineOptions)
	if err != nil {
		var pipelineCanceledError perrors.PipelineCanceledError
		var runError perrors.RunError
		if errors.As(err, &pipelineCanceledError) {
			logger.Warnf("%s: pipeline execution has been canceled: %s", pipelineId, pipelineCanceledError.Error())
		} else if errors.As(err, &runError) {
			logger.Warnf("%s: pipeline execution ended with error from child process: %s", runError.Error())
		} else {
			logger.Errorf("%s: error during run step: %s", pipelineId, err.Error())
		}
		return
	}
}

func runStep(ctx context.Context, cacheService cache.Cache, paths *fs_tool.LifeCyclePaths, pipelineId uuid.UUID, isUnitTest bool, sdkEnv *environment.BeamEnvs, pipelineOptions string) error {
	errorChannel, successChannel := createStatusChannels()
	stopReadLogsChannel := make(chan bool, 1)
	finishReadLogsChannel := make(chan bool, 1)

	var executorBuilder *executors.ExecutorBuilder
	err := error(nil)
	if isUnitTest {
		executorBuilder, err = builder.TestRunner(ctx, paths, sdkEnv)
	} else {
		executorBuilder, err = builder.Runner(ctx, paths, utils.ReduceWhiteSpacesToSinge(pipelineOptions), sdkEnv)
	}
	if err != nil {
		if processingErr := processSetupError(err, pipelineId, cacheService); processingErr != nil {
			return processingErr
		}
		return err
	}

	executor := executorBuilder.Build()
	logger.Infof("%s: Run()/Test() ...\n", pipelineId)
	runCmd := getExecuteCmd(isUnitTest, &executor, ctx)
	var runError bytes.Buffer
	runOutput := streaming.RunOutputWriter{Ctx: ctx, CacheService: cacheService, PipelineId: pipelineId}
	go readLogFile(ctx, cacheService, paths.AbsoluteLogFilePath, pipelineId, stopReadLogsChannel, finishReadLogsChannel)
	go readGraphFile(ctx, cacheService, paths.AbsoluteGraphFilePath, pipelineId)

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
	ok, err := reconcileBackgroundTask(ctx, pipelineId, cacheService, successChannel)
	if err != nil {
		return err
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
		processingErr := processRunError(errorChannel, runError.Bytes(), pipelineId, cacheService, stopReadLogsChannel, finishReadLogsChannel)
		if processingErr != nil {
			return processingErr
		}
		return perrors.RunError{Log: runError}
	}
	// Run step is finished and code is executed
	err = processRunSuccess(pipelineId, cacheService, stopReadLogsChannel, finishReadLogsChannel)
	if err != nil {
		return err
	}
	return nil
}

func compileStep(ctx context.Context, cacheService cache.Cache, paths *fs_tool.LifeCyclePaths, pipelineId uuid.UUID, sdkEnv *environment.BeamEnvs, isUnitTest bool) error {
	errorChannel, successChannel := createStatusChannels()
	// This condition is used for cases when the playground doesn't compile source files. For the Python code and the Go Unit Tests
	if sdkEnv.ApacheBeamSdk == pb.Sdk_SDK_PYTHON || sdkEnv.ApacheBeamSdk == pb.Sdk_SDK_SCIO || (sdkEnv.ApacheBeamSdk == pb.Sdk_SDK_GO && isUnitTest) {
		if err := processCompileSuccess([]byte(""), pipelineId, cacheService); err != nil {
			return err
		}
	} else { // in case of Java, Go (not unit test), Scala - need compile step
		executorBuilder, err := builder.Compiler(paths, sdkEnv)
		if err != nil {
			logger.Errorf("compileStep(): failed creating ExecutorBuilder = %v", executorBuilder)
			return nil
		}
		executor := executorBuilder.Build()
		logger.Infof("%s: Compile() ...\n", pipelineId)
		compileCmd := executor.Compile(ctx)
		var compileError bytes.Buffer
		var compileOutput bytes.Buffer
		runCmdWithOutput(compileCmd, &compileOutput, &compileError, successChannel, errorChannel)

		// Start of the monitoring of background tasks (compile step/cancellation/timeout)
		ok, err := reconcileBackgroundTask(ctx, pipelineId, cacheService, successChannel)
		if err != nil {
			return err
		}
		if !ok { // Compile step is finished, but code couldn't be compiled (some typos for example)
			err := <-errorChannel
			processingErr := processErrorWithSavingOutput(err, compileError.Bytes(), pipelineId, cache.CompileOutput, cacheService, "Compile", pb.Status_STATUS_COMPILE_ERROR)
			if processingErr != nil {
				return processingErr
			}
			return perrors.CompilationError{Reason: err.Error()}
		} // Compile step is finished and code is compiled
		if err := processCompileSuccess(compileOutput.Bytes(), pipelineId, cacheService); err != nil {
			return err
		}
	}
	return nil
}

func prepareStep(ctx context.Context, cacheService cache.Cache, paths *fs_tool.LifeCyclePaths, pipelineId uuid.UUID, sdkEnv *environment.BeamEnvs, validationResults *sync.Map, prepareParams map[string]string) error {
	errorChannel, successChannel := createStatusChannels()
	executorBuilder, err := builder.Preparer(paths, sdkEnv, validationResults, prepareParams)
	if err != nil {
		if processingErr := processSetupError(err, pipelineId, cacheService); processingErr != nil {
			return processingErr
		}
		return err
	}
	executor := executorBuilder.Build()
	logger.Infof("%s: Prepare() ...\n", pipelineId)
	prepareFunc := executor.Prepare()
	go prepareFunc(successChannel, errorChannel, validationResults)

	// Start of the monitoring of background tasks (prepare function/cancellation/timeout)
	ok, err := reconcileBackgroundTask(ctx, pipelineId, cacheService, successChannel)
	if err != nil {
		return err
	}
	if !ok {
		err := <-errorChannel
		// Prepare step is finished, but code couldn't be prepared (some error during prepare step)
		processingErr := processErrorWithSavingOutput(err, []byte(err.Error()), pipelineId, cache.PreparationOutput, cacheService, "Prepare", pb.Status_STATUS_PREPARATION_ERROR)
		if processingErr != nil {
			return processingErr
		}
		return err
	}
	// Prepare step is finished and code is prepared
	if err := processSuccess(pipelineId, cacheService, "Prepare", pb.Status_STATUS_COMPILING); err != nil {
		return err
	}
	return nil
}

func validateStep(ctx context.Context, cacheService cache.Cache, paths *fs_tool.LifeCyclePaths, pipelineId uuid.UUID, sdkEnv *environment.BeamEnvs, validationResults *sync.Map) error {
	errorChannel, successChannel := createStatusChannels()
	executorBuilder, err := builder.Validator(paths, sdkEnv)
	if err != nil {
		if processingError := processSetupError(err, pipelineId, cacheService); processingError != nil {
			return processingError
		}
		return err
	}
	executor := executorBuilder.Build()
	logger.Infof("%s: Validate() ...\n", pipelineId)
	validateFunc := executor.Validate()
	go validateFunc(successChannel, errorChannel, validationResults)

	// Start of the monitoring of background tasks (validate function/cancellation/timeout)
	ok, err := reconcileBackgroundTask(ctx, pipelineId, cacheService, successChannel)
	if err != nil {
		return err
	}
	if !ok {
		err := <-errorChannel
		// Validate step is finished, but code isn't valid
		processingErr := processErrorWithSavingOutput(err, []byte(err.Error()), pipelineId, cache.ValidationOutput, cacheService, "Validate", pb.Status_STATUS_VALIDATION_ERROR)
		if processingErr != nil {
			return processingErr
		}
		return err
	}

	// Validate step is finished and code is valid
	if err := processSuccess(pipelineId, cacheService, "Validate", pb.Status_STATUS_PREPARING); err != nil {
		return err
	}
	return err
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
func processSetupError(err error, pipelineId uuid.UUID, cacheService cache.Cache) error {
	logger.Errorf("%s: error during setup builder: %s\n", pipelineId, err.Error())
	if err = utils.SetToCache(cacheService, pipelineId, cache.Status, pb.Status_STATUS_ERROR); err != nil {
		logger.Errorf("%s: error during saving error message: %s", pipelineId, err)
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
		return "", perrors.NotFoundError(errorTitle, "Error during getting output")
	}
	stringValue, converted := value.(string)
	if !converted {
		logger.Errorf("%s: couldn't convert value to string: %s", key, value)
		return "", perrors.InternalError(errorTitle, "Error during getting output")
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
		return pb.Status_STATUS_UNSPECIFIED, perrors.NotFoundError(errorTitle, "Error during getting status")
	}
	statusValue, converted := value.(pb.Status)
	if !converted {
		logger.Errorf("%s: couldn't convert value to correct status enum: %s", key, value)
		return pb.Status_STATUS_UNSPECIFIED, perrors.InternalError(errorTitle, "Error during getting status")
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
		return 0, perrors.NotFoundError(errorTitle, "Error during getting pagination value")
	}
	convertedValue, converted := value.(float64)
	if !converted {
		logger.Errorf("%s: couldn't convert value to float64. value: %s type %s", key, value, reflect.TypeOf(value))
		return 0, perrors.InternalError(errorTitle, "Error during getting pagination value")
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
		return "", perrors.NotFoundError(errorTitle, "Error during getting graph")
	}
	stringValue, converted := value.(string)
	if !converted {
		logger.Errorf("%s: couldn't convert value to string. value: %s type %s", key, value, reflect.TypeOf(value))
		return "", perrors.InternalError(errorTitle, "Error during getting graph")
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
//
//	during step processing - returns true.
//
// If cmd operation (Validate/Prepare/Compile/Run/RunTest) finishes successfully but with some error
//
//	during step processing - returns false.
func reconcileBackgroundTask(pipelineLifeCycleCtx context.Context, pipelineId uuid.UUID, cacheService cache.Cache, successChannel chan bool) (bool, error) {
	select {
	case <-pipelineLifeCycleCtx.Done():
		contextErr := pipelineLifeCycleCtx.Err()
		switch contextErr {
		case context.DeadlineExceeded:
			if err := finishByTimeout(pipelineId, cacheService); err != nil {
				return false, fmt.Errorf("error during context timeout processing: %s", err.Error())
			}
			return false, perrors.PipelineCanceledError{Reason: fmt.Sprintf("code processing context timeout")}
		case context.Canceled:
			if err := processCancel(cacheService, pipelineId); err != nil {
				return false, fmt.Errorf("error during cancellation processing: %s", err.Error())
			}
			return false, perrors.PipelineCanceledError{Reason: "code processing was canceled"}
		default:
			return false, fmt.Errorf("code processing cancelled due to unexpected reason: %s", contextErr.Error())
		}
	case ok := <-successChannel:
		return ok, nil
	}
}

// cancelCheck checks cancel flag for code processing.
// If cancel flag doesn't exist in cache continue working.
// If context is done it means that the code processing was finished (successfully/with error/timeout). Return.
// If cancel flag exists, and it is true it means that the code processing was canceled. Set true to cancelChannel and return.
func cancelCheck(ctx context.Context, pipelineId uuid.UUID, cancelFunc context.CancelFunc, cacheService cache.Cache) {
	ticker := time.NewTicker(pauseDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Use background context for the cache operation to avoid failure when the main context is timed out.
			// Timeouts are handled by the Redis client internally.
			cancel, err := cacheService.GetValue(context.Background(), pipelineId, cache.Canceled)
			// Only cancel if got the boolean value from the cache. Do not cancel if the value is nil or error happened.
			if err != nil {
				logger.Errorf("%s: Error during getting value from the cache: %s", pipelineId, err.Error())
			} else if cancel != nil && cancel.(bool) {
				cancelFunc()
				return
			}
		}
	}
}

// readGraphFile reads graph from the file and keeps it to the cache.
// If context is done it means that the code processing was finished (successfully/with error/timeout).
// Write graph to the cache if this in the file.
// In other case each pauseDuration checks that graph file exists or not and try to save it to the cache.
func readGraphFile(pipelineLifeCycleCtx context.Context, cacheService cache.Cache, graphFilePath string, pipelineId uuid.UUID) {
	ticker := time.NewTicker(pauseDuration)
	defer ticker.Stop()
	for {
		select {
		// waiting when graph file appears
		case <-ticker.C:
			if _, err := os.Stat(graphFilePath); err == nil {
				graph, err := os.ReadFile(graphFilePath)
				if err != nil {
					logger.Errorf("%s: Error during saving graph to the file: %s", pipelineId, err.Error())
				}
				_ = utils.SetToCache(cacheService, pipelineId, cache.Graph, string(graph))
			}
		// in case of timeout or cancel
		case <-pipelineLifeCycleCtx.Done():
			if _, err := os.Stat(graphFilePath); err == nil {
				graph, err := os.ReadFile(graphFilePath)
				if err != nil {
					logger.Errorf("%s: Error during saving graph to the file: %s", pipelineId, err.Error())
				}
				_ = utils.SetToCache(cacheService, pipelineId, cache.Graph, string(graph))
			}
			return
		}
	}
}

// readLogFile reads logs from the log file and keeps it to the cache.
// If context is done it means that the code processing was finished (successfully/with error/timeout). Write last logs to the cache.
// If <-stopReadLogsChannel it means that the code processing was finished (canceled/timeout)
//
//	and it waits until the method stops the work to change status to the pb.Status_STATUS_FINISHED. Write last logs
//	to the cache and set value to the finishReadLogChannel channel to unblock the code processing.
//
// In other case each pauseDuration write to cache logs of the code processing.
func readLogFile(pipelineLifeCycleCtx context.Context, cacheService cache.Cache, logFilePath string, pipelineId uuid.UUID, stopReadLogsChannel, finishReadLogChannel chan bool) {
	ticker := time.NewTicker(pauseDuration)
	defer ticker.Stop()
	for {
		select {
		// in case of timeout or cancel
		case <-pipelineLifeCycleCtx.Done():
			_ = finishReadLogFile(ticker, cacheService, logFilePath, pipelineId)
			return
		// in case of pipeline finish successfully or has error on the run step
		case <-stopReadLogsChannel:
			_ = finishReadLogFile(ticker, cacheService, logFilePath, pipelineId)
			finishReadLogChannel <- true
			return
		case <-ticker.C:
			_ = writeLogsToCache(cacheService, logFilePath, pipelineId)
		}
	}
}

// finishReadLogFile is used to read logs file for the last time
func finishReadLogFile(ticker *time.Ticker, cacheService cache.Cache, logFilePath string, pipelineId uuid.UUID) error {
	ticker.Stop()
	return writeLogsToCache(cacheService, logFilePath, pipelineId)
}

// writeLogsToCache write all logs from the log file to the cache.
// If log file doesn't exist, return nil.
//
//	Reading logs works as a parallel with code processing so when program tries to read file
//	it could be that the file doesn't exist yet.
//
// If log file exists, read all from the log file and keep it to the cache using cache.Logs subKey.
// If some error occurs, log the error and return the error.
func writeLogsToCache(cacheService cache.Cache, logFilePath string, pipelineId uuid.UUID) error {
	if _, err := os.Stat(logFilePath); os.IsNotExist(err) {
		return nil
	}
	logs, err := os.ReadFile(logFilePath)
	if err != nil {
		logger.Errorf("%s: writeLogsToCache(): error during read from logs file: %s", pipelineId, err.Error())
		return err
	}
	return utils.SetToCache(cacheService, pipelineId, cache.Logs, string(logs))
}

// DeleteResources removes all prepared resources for received LifeCycle
func DeleteResources(pipelineId uuid.UUID, lc *fs_tool.LifeCycle) {
	logger.Infof("%s: DeleteResources() ...\n", pipelineId)
	if err := lc.DeleteFolders(); err != nil {
		logger.Error("%s: DeleteResources(): %s\n", pipelineId, err.Error())
	}
	lc.StopEmulators()
	logger.Infof("%s: DeleteResources() complete\n", pipelineId)
	logger.Infof("%s: complete\n", pipelineId)

}

// finishByTimeout is used in case of runCode method finished by timeout
func finishByTimeout(pipelineId uuid.UUID, cacheService cache.Cache) error {
	logger.Warnf("%s: code processing finishes because of timeout\n", pipelineId)

	// set to cache pipelineId: cache.SubKey_Status: Status_STATUS_RUN_TIMEOUT
	return utils.SetToCache(cacheService, pipelineId, cache.Status, pb.Status_STATUS_RUN_TIMEOUT)
}

// processErrorWithSavingOutput processes error with saving to cache received error output.
func processErrorWithSavingOutput(err error, errorOutput []byte, pipelineId uuid.UUID, subKey cache.SubKey, cacheService cache.Cache, errorTitle string, newStatus pb.Status) error {
	logger.Warnf("%s: %s(): err: %s, output: %s\n", pipelineId, errorTitle, err.Error(), errorOutput)

	if err := utils.SetToCache(cacheService, pipelineId, subKey, fmt.Sprintf("error: %s\noutput: %s", err.Error(), errorOutput)); err != nil {
		logger.Errorf("%s: failed to save error message to cache: %s", pipelineId, err.Error())
		return err
	}

	if err = utils.SetToCache(cacheService, pipelineId, cache.Status, newStatus); err != nil {
		logger.Errorf("%s: failed to save status to cache: %s", pipelineId, err.Error())
		return err
	}
	return nil
}

// processRunError processes error received during processing run step.
// This method sets error output to the cache and after that sets value to channel to stop goroutine which writes logs.
//
//	After receiving a signal that goroutine was finished (read value from finishReadLogsChannel) this method
//	sets corresponding status to the cache.
func processRunError(errorChannel chan error, errorOutput []byte, pipelineId uuid.UUID, cacheService cache.Cache, stopReadLogsChannel, finishReadLogsChannel chan bool) error {
	err := <-errorChannel
	logger.Warnf("%s: Run(): err: %s, output: %s\n", pipelineId, err.Error(), errorOutput)

	if err := utils.SetToCache(cacheService, pipelineId, cache.RunError, fmt.Sprintf("error: %s\noutput: %s", err.Error(), string(errorOutput))); err != nil {
		return err
	}

	stopReadLogsChannel <- true
	<-finishReadLogsChannel

	return utils.SetToCache(cacheService, pipelineId, cache.Status, pb.Status_STATUS_RUN_ERROR)
}

// processSuccess processes case after successful process validation or preparation steps.
// This method sets corresponding status to the cache.
func processSuccess(pipelineId uuid.UUID, cacheService cache.Cache, successTitle string, newStatus pb.Status) error {
	logger.Infof("%s: %s(): finish\n", pipelineId, successTitle)

	return utils.SetToCache(cacheService, pipelineId, cache.Status, newStatus)
}

// processCompileSuccess processes case after successful compile step.
// This method sets output of the compile step, sets empty string as output of the run step and
//
//	sets corresponding status to the cache.
func processCompileSuccess(output []byte, pipelineId uuid.UUID, cacheService cache.Cache) error {
	logger.Infof("%s: Compile() finish\n", pipelineId)

	if err := utils.SetToCache(cacheService, pipelineId, cache.CompileOutput, string(output)); err != nil {
		return err
	}
	if err := utils.SetToCache(cacheService, pipelineId, cache.RunOutput, ""); err != nil {
		return err
	}
	if err := utils.SetToCache(cacheService, pipelineId, cache.RunError, ""); err != nil {
		return err
	}
	if err := utils.SetToCache(cacheService, pipelineId, cache.Logs, ""); err != nil {
		return err
	}
	if err := utils.SetToCache(cacheService, pipelineId, cache.Graph, ""); err != nil {
		return err
	}
	return utils.SetToCache(cacheService, pipelineId, cache.Status, pb.Status_STATUS_EXECUTING)
}

// processRunSuccess processes case after successful run step.
// This method sets value to channel to stop goroutine which writes logs.
//
//	After receiving a signal that goroutine was finished (read value from finishReadLogsChannel) this method
//	sets corresponding status to the cache.
func processRunSuccess(pipelineId uuid.UUID, cacheService cache.Cache, stopReadLogsChannel, finishReadLogsChannel chan bool) error {
	logger.Infof("%s: Run() finish\n", pipelineId)

	stopReadLogsChannel <- true
	<-finishReadLogsChannel

	return utils.SetToCache(cacheService, pipelineId, cache.Status, pb.Status_STATUS_FINISHED)
}

// processCancel process case when code processing was canceled
func processCancel(cacheService cache.Cache, pipelineId uuid.UUID) error {
	logger.Infof("%s: was canceled\n", pipelineId)

	// set to cache pipelineId: cache.SubKey_Status: pb.Status_STATUS_CANCELED
	return utils.SetToCache(cacheService, pipelineId, cache.Status, pb.Status_STATUS_CANCELED)
}
