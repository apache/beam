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
)

const (
	javaLogConfigFileName        = "logging.properties"
	javaLogConfigFilePlaceholder = "{logConfigFile}"
)

// SetupExecutorBuilder return executor with set args for validator, preparator, compiler and runner
func SetupExecutorBuilder(lcDto fs_tool.LifeCycleDTO, pipelineOptions string, sdkEnv *environment.BeamEnvs) (*executors.ExecutorBuilder, error) {
	sdk := sdkEnv.ApacheBeamSdk

	if sdk == pb.Sdk_SDK_JAVA {
		pipelineOptions = utils.ReplaceSpacesWithEquals(pipelineOptions)
	}

	val, err := utils.GetValidators(sdk, lcDto.GetAbsoluteSourceFilePath())
	if err != nil {
		return nil, err
	}
	prep, err := utils.GetPreparators(sdk, lcDto.GetAbsoluteSourceFilePath())
	if err != nil {
		return nil, err
	}
	executorConfig := sdkEnv.ExecutorConfig
	builder := executors.NewExecutorBuilder().
		WithExecutableFileName(lcDto.GetAbsoluteExecutableFilePath()).
		WithWorkingDir(lcDto.GetAbsoluteBaseFolderPath()).
		WithValidator().
		WithSdkValidators(val).
		WithPreparator().
		WithSdkPreparators(prep).
		WithCompiler().
		WithCommand(executorConfig.CompileCmd).
		WithArgs(executorConfig.CompileArgs).
		WithFileName(lcDto.GetAbsoluteSourceFilePath()).
		WithRunner().
		WithCommand(executorConfig.RunCmd).
		WithArgs(executorConfig.RunArgs).
		WithPipelineOptions(strings.Split(pipelineOptions, " ")).
		WithTestRunner().
		WithCommand(executorConfig.TestCmd).
		WithArgs(executorConfig.TestArgs).
		WithWorkingDir(lcDto.GetAbsoluteSourceFileFolderPath()).
		ExecutorBuilder

	switch sdk {
	case pb.Sdk_SDK_JAVA: // Executable name for java class will be known after compilation
		args := make([]string, 0)
		for _, arg := range executorConfig.RunArgs {
			if strings.Contains(arg, javaLogConfigFilePlaceholder) {
				logConfigFilePath := filepath.Join(lcDto.GetAbsoluteBaseFolderPath(), javaLogConfigFileName)
				arg = strings.Replace(arg, javaLogConfigFilePlaceholder, logConfigFilePath, 1)
			}
			args = append(args, arg)
		}
		builder = builder.WithRunner().WithArgs(args).ExecutorBuilder
		builder = builder.WithTestRunner().WithWorkingDir(lcDto.GetAbsoluteBaseFolderPath()).ExecutorBuilder //change directory for unit test
	case pb.Sdk_SDK_GO: //go run command is executable file itself
		builder = builder.
			WithExecutableFileName("").
			WithRunner().
			WithCommand(lcDto.GetAbsoluteExecutableFilePath()).ExecutorBuilder
	case pb.Sdk_SDK_PYTHON:
		builder = *builder.WithExecutableFileName(lcDto.GetAbsoluteExecutableFilePath())
	case pb.Sdk_SDK_SCIO:
		return nil, fmt.Errorf("SCIO is not supported yet")
	default:
		return nil, fmt.Errorf("incorrect sdk: %s", sdkEnv.ApacheBeamSdk)
	}
	return &builder, nil
}

// GetFileNameFromFolder return a name of the first file in a specified folder
func GetFileNameFromFolder(folderAbsolutePath string) string {
	files, _ := filepath.Glob(fmt.Sprintf("%s/*%s", folderAbsolutePath, fs_tool.JavaSourceFileExtension))
	return files[0]
}
