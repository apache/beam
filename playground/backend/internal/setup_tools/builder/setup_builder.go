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

package builder

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/executors"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"beam.apache.org/playground/backend/internal/utils"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
)

const (
	javaLogConfigFileName        = "logging.properties"
	javaLogConfigFilePlaceholder = "{logConfigFile}"
)

// Validator return executor with set args for validator
func Validator(paths *fs_tool.LifeCyclePaths, sdkEnv *environment.BeamEnvs) (*executors.ExecutorBuilder, error) {
	sdk := sdkEnv.ApacheBeamSdk
	val, err := utils.GetValidators(sdk, paths.AbsoluteSourceFilePath)
	if err != nil {
		return nil, err
	}
	builder := executors.NewExecutorBuilder().
		WithValidator().
		WithSdkValidators(val).
		ExecutorBuilder
	return &builder, err
}

// Preparer return executor with set args for preparer
func Preparer(paths *fs_tool.LifeCyclePaths, sdkEnv *environment.BeamEnvs, valResults *sync.Map) (*executors.ExecutorBuilder, error) {
	sdk := sdkEnv.ApacheBeamSdk
	prep, err := utils.GetPreparers(sdk, paths.AbsoluteSourceFilePath, valResults)
	if err != nil {
		return nil, err
	}
	builder := executors.NewExecutorBuilder().
		WithPreparer().
		WithSdkPreparers(prep).
		ExecutorBuilder
	return &builder, err
}

// Compiler return executor with set args for compiler
func Compiler(paths *fs_tool.LifeCyclePaths, sdkEnv *environment.BeamEnvs) *executors.ExecutorBuilder {
	sdk := sdkEnv.ApacheBeamSdk
	executorConfig := sdkEnv.ExecutorConfig
	builder := executors.NewExecutorBuilder().
		WithCompiler().
		WithCommand(executorConfig.CompileCmd).
		WithWorkingDir(paths.AbsoluteBaseFolderPath).
		WithArgs(executorConfig.CompileArgs).
		WithFileName(paths.AbsoluteSourceFilePath).
		ExecutorBuilder

	switch sdk {
	case pb.Sdk_SDK_JAVA:
		builder.
			WithCompiler().
			WithFileName(GetFirstFileFromFolder(paths.AbsoluteSourceFileFolderPath))
	}
	return &builder
}

// Runner return executor with set args for runner
func Runner(paths *fs_tool.LifeCyclePaths, pipelineOptions string, sdkEnv *environment.BeamEnvs) (*executors.ExecutorBuilder, error) {
	sdk := sdkEnv.ApacheBeamSdk

	if sdk == pb.Sdk_SDK_JAVA {
		pipelineOptions = utils.ReplaceSpacesWithEquals(pipelineOptions)
	}
	executorConfig := sdkEnv.ExecutorConfig
	builder := executors.NewExecutorBuilder().
		WithRunner().
		WithWorkingDir(paths.AbsoluteBaseFolderPath).
		WithCommand(executorConfig.RunCmd).
		WithArgs(executorConfig.RunArgs).
		WithPipelineOptions(strings.Split(pipelineOptions, " ")).
		ExecutorBuilder

	switch sdk {
	case pb.Sdk_SDK_JAVA: // Executable name for java class is known after compilation
		args := replaceLogPlaceholder(paths, executorConfig)
		className, err := paths.ExecutableName(paths.AbsoluteExecutableFileFolderPath)
		if err != nil {
			return nil, fmt.Errorf("no executable file name found for JAVA pipeline at %s", paths.AbsoluteExecutableFileFolderPath)
		}
		builder = builder.
			WithRunner().
			WithArgs(args).
			WithExecutableFileName(className).
			ExecutorBuilder
	case pb.Sdk_SDK_GO: //go run command is executable file itself
		builder = builder.
			WithRunner().
			WithExecutableFileName("").
			WithCommand(paths.AbsoluteExecutableFilePath).
			ExecutorBuilder
	case pb.Sdk_SDK_PYTHON:
		builder = builder.
			WithRunner().
			WithExecutableFileName(paths.AbsoluteExecutableFilePath).
			ExecutorBuilder
	}
	return &builder, nil
}

// TestRunner return executor with set args for runner
func TestRunner(paths *fs_tool.LifeCyclePaths, sdkEnv *environment.BeamEnvs) (*executors.ExecutorBuilder, error) {
	sdk := sdkEnv.ApacheBeamSdk
	executorConfig := sdkEnv.ExecutorConfig
	builder := executors.NewExecutorBuilder().
		WithTestRunner().
		WithExecutableFileName(paths.AbsoluteExecutableFilePath).
		WithCommand(executorConfig.TestCmd).
		WithArgs(executorConfig.TestArgs).
		WithWorkingDir(paths.AbsoluteSourceFileFolderPath).
		ExecutorBuilder

	switch sdk {
	case pb.Sdk_SDK_JAVA: // Executable name for java class is known after compilation
		className, err := paths.ExecutableName(paths.AbsoluteExecutableFileFolderPath)
		if err != nil {
			return nil, fmt.Errorf("no executable file name found for JAVA pipeline at %s", paths.AbsoluteExecutableFileFolderPath)
		}
		builder = builder.WithTestRunner().
			WithExecutableFileName(className).
			WithWorkingDir(paths.AbsoluteBaseFolderPath).
			ExecutorBuilder //change directory for unit test
	}
	return &builder, nil
}

// replaceLogPlaceholder replaces placeholder for log for JAVA SDK
func replaceLogPlaceholder(paths *fs_tool.LifeCyclePaths, executorConfig *environment.ExecutorConfig) []string {
	args := make([]string, 0)
	for _, arg := range executorConfig.RunArgs {
		if strings.Contains(arg, javaLogConfigFilePlaceholder) {
			logConfigFilePath := filepath.Join(paths.AbsoluteBaseFolderPath, javaLogConfigFileName)
			arg = strings.Replace(arg, javaLogConfigFilePlaceholder, logConfigFilePath, 1)
		}
		args = append(args, arg)
	}
	return args
}

// GetFirstFileFromFolder return a name of the first file in a specified folder
func GetFirstFileFromFolder(folderAbsolutePath string) string {
	files, _ := filepath.Glob(fmt.Sprintf("%s/*%s", folderAbsolutePath, fs_tool.JavaSourceFileExtension))
	return files[0]
}
