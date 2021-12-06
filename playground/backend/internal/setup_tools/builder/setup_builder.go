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
func SetupExecutorBuilder(srcFilePath, baseFolderPath, execFilePath string, sdkEnv *environment.BeamEnvs) (*executors.ExecutorBuilder, error) {
	sdk := sdkEnv.ApacheBeamSdk

	val, err := utils.GetValidators(sdk, srcFilePath)
	if err != nil {
		return nil, err
	}
	prep, err := utils.GetPreparators(sdk, srcFilePath)
	if err != nil {
		return nil, err
	}
	executorConfig := sdkEnv.ExecutorConfig
	builder := executors.NewExecutorBuilder().
		WithExecutableFileName(execFilePath).
		WithWorkingDir(baseFolderPath).
		WithValidator().
		WithSdkValidators(val).
		WithPreparator().
		WithSdkPreparators(prep).
		WithCompiler().
		WithCommand(executorConfig.CompileCmd).
		WithArgs(executorConfig.CompileArgs).
		WithFileName(srcFilePath).
		WithRunner().
		WithCommand(executorConfig.RunCmd).
		WithArgs(executorConfig.RunArgs).
		WithTestRunner().
		WithCommand(executorConfig.TestCmd).
		WithArgs(executorConfig.TestArgs).
		ExecutorBuilder

	switch sdk {
	case pb.Sdk_SDK_JAVA: // Executable name for java class will be known after compilation
		args := make([]string, 0)
		for _, arg := range executorConfig.RunArgs {
			if strings.Contains(arg, javaLogConfigFilePlaceholder) {
				logConfigFilePath := filepath.Join(baseFolderPath, javaLogConfigFileName)
				arg = strings.Replace(arg, javaLogConfigFilePlaceholder, logConfigFilePath, 1)
			}
			args = append(args, arg)
		}
		builder = builder.WithRunner().WithArgs(args).ExecutorBuilder
	case pb.Sdk_SDK_GO: //go run command is executable file itself
		builder = builder.
			WithExecutableFileName("").
			WithRunner().
			WithCommand(execFilePath).ExecutorBuilder
	default:
		return nil, fmt.Errorf("incorrect sdk: %s", sdkEnv.ApacheBeamSdk)
	}
	return &builder, nil
}
