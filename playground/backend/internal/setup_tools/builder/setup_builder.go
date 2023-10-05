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
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/executors"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"beam.apache.org/playground/backend/internal/preparers"
	"beam.apache.org/playground/backend/internal/utils"
	"beam.apache.org/playground/backend/internal/validators"
)

const (
	javaLogConfigFileName        = "logging.properties"
	javaLogConfigFilePlaceholder = "{logConfigFile}"
)

// Validator return executor with set args for validator
func Validator(paths *fs_tool.LifeCyclePaths, sdkEnv *environment.BeamEnvs) (*executors.ExecutorBuilder, error) {
	sdk := sdkEnv.ApacheBeamSdk
	val, err := validators.GetValidators(sdk, paths.AbsoluteSourceFilePath)
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
func Preparer(paths *fs_tool.LifeCyclePaths, sdkEnv *environment.BeamEnvs, valResults *sync.Map, prepareParams map[string]string) (*executors.ExecutorBuilder, error) {
	sdk := sdkEnv.ApacheBeamSdk
	prep, err := preparers.GetPreparers(sdk, paths.AbsoluteSourceFilePath, valResults, prepareParams)
	if err != nil {
		return nil, err
	}
	builder := executors.NewExecutorBuilder().
		WithPreparer().
		WithSdkPreparers(prep).
		ExecutorBuilder
	return &builder, nil
}

// Compiler return executor with set args for compiler
func Compiler(paths *fs_tool.LifeCyclePaths, sdkEnv *environment.BeamEnvs) (*executors.ExecutorBuilder, error) {
	sdk := sdkEnv.ApacheBeamSdk
	executorConfig := sdkEnv.ExecutorConfig
	builder := executors.NewExecutorBuilder().
		WithCompiler().
		WithCommand(executorConfig.CompileCmd).
		WithWorkingDir(paths.AbsoluteBaseFolderPath).
		WithArgs(executorConfig.CompileArgs).
		ExecutorBuilder

	switch sdk {
	case pb.Sdk_SDK_JAVA:
		javaSources, err := GetFilesFromFolder(paths.AbsoluteSourceFileFolderPath, fs_tool.JavaSourceFileExtension)
		if err != nil {
			return nil, err
		}
		builder = builder.
			WithCompiler().
			WithFileNames(javaSources...).
			ExecutorBuilder
	case pb.Sdk_SDK_GO:
		goSources, err := GetFilesFromFolder(paths.AbsoluteSourceFileFolderPath, fs_tool.GoSourceFileExtension)
		if err != nil {
			return nil, err
		}
		builder = builder.
			WithCompiler().
			WithFileNames(goSources...).
			ExecutorBuilder
	default:
		builder = builder.
			WithCompiler().
			WithFileNames(paths.AbsoluteSourceFilePath).
			ExecutorBuilder
	}
	return &builder, nil
}

// Runner return executor with set args for runner
func Runner(ctx context.Context, paths *fs_tool.LifeCyclePaths, pipelineOptions string, sdkEnv *environment.BeamEnvs) (*executors.ExecutorBuilder, error) {
	sdk := sdkEnv.ApacheBeamSdk

	if sdk == pb.Sdk_SDK_JAVA || sdk == pb.Sdk_SDK_SCIO {
		pipelineOptions = utils.ReplaceSpacesWithEquals(pipelineOptions)
	}
	executorConfig := sdkEnv.ExecutorConfig
	builder := executors.NewExecutorBuilder().
		WithRunner().
		WithWorkingDir(paths.AbsoluteSourceFileFolderPath).
		WithCommand(executorConfig.RunCmd).
		WithArgs(executorConfig.RunArgs).
		ExecutorBuilder

	switch sdk {
	case pb.Sdk_SDK_JAVA: // Executable name for java class is known after compilation
		args := replaceLogPlaceholder(paths, executorConfig)
		className, err := paths.FindExecutableName(ctx, paths.AbsoluteExecutableFileFolderPath)
		if err != nil {
			return nil, fmt.Errorf("no executable file name found for JAVA pipeline at %s", paths.AbsoluteExecutableFileFolderPath)
		}
		builder = builder.
			WithRunner().
			WithArgs(args).
			WithExecutableFileNames(className).
			WithPipelineOptions(strings.Split(pipelineOptions, " ")).
			ExecutorBuilder
	case pb.Sdk_SDK_GO: //go run command is executable file itself
		builder = builder.
			WithRunner().
			WithCommand(paths.AbsoluteExecutableFilePath).
			WithPipelineOptions(strings.Split(pipelineOptions, " ")).
			ExecutorBuilder
	case pb.Sdk_SDK_PYTHON:
		builder = builder.
			WithRunner().
			WithExecutableFileNames(paths.AbsoluteExecutableFilePath).
			WithPipelineOptions(strings.Split(pipelineOptions, " ")).
			ExecutorBuilder
	case pb.Sdk_SDK_SCIO:
		className, err := paths.FindExecutableName(ctx, paths.AbsoluteBaseFolderPath)
		if err != nil {
			return nil, fmt.Errorf("no executable file name found for SCIO pipeline at %s", paths.AbsoluteBaseFolderPath)
		}
		stringArg := fmt.Sprintf("%s %s %s", executorConfig.RunArgs[0], className, pipelineOptions)
		builder = builder.
			WithRunner().
			WithWorkingDir(paths.ProjectDir).
			WithArgs([]string{stringArg}).
			ExecutorBuilder
	}
	return &builder, nil
}

// TestRunner return executor with set args for runner
func TestRunner(ctx context.Context, paths *fs_tool.LifeCyclePaths, sdkEnv *environment.BeamEnvs) (*executors.ExecutorBuilder, error) {
	sdk := sdkEnv.ApacheBeamSdk
	executorConfig := sdkEnv.ExecutorConfig
	builder := executors.NewExecutorBuilder().
		WithTestRunner().
		WithCommand(executorConfig.TestCmd).
		WithArgs(executorConfig.TestArgs).
		WithWorkingDir(paths.AbsoluteSourceFileFolderPath).
		ExecutorBuilder

	switch sdk {
	case pb.Sdk_SDK_JAVA: // Executable name for java class is known after compilation
		className, err := paths.FindTestExecutableName(ctx, paths.AbsoluteExecutableFileFolderPath)
		if err != nil {
			return nil, fmt.Errorf("no executable file name found for JAVA pipeline at %s", paths.AbsoluteExecutableFileFolderPath)
		}
		builder = builder.WithTestRunner().
			WithExecutableFileName(className).
			WithWorkingDir(paths.AbsoluteBaseFolderPath).
			ExecutorBuilder //change directory for unit test
	case pb.Sdk_SDK_GO:
		builder = builder.WithTestRunner().
			WithExecutableFileName(paths.AbsoluteSourceFileFolderPath). // run all tests in folder
			ExecutorBuilder
	default:
		builder = builder.WithTestRunner().
			WithExecutableFileName(paths.AbsoluteExecutableFilePath).
			ExecutorBuilder
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
func GetFilesFromFolder(folderAbsolutePath string, extension string) ([]string, error) {
	return filepath.Glob(fmt.Sprintf("%s/*%s", folderAbsolutePath, extension))
}
