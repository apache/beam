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

package compile_builder

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/executors"
	"beam.apache.org/playground/backend/internal/utils"
)

// Setup returns executors.CompileBuilder setup it according to sdk
func Setup(filePath, filesFolderPath string, sdk pb.Sdk, executorConfig *environment.ExecutorConfig) (*executors.CompileBuilder, error) {
	val, err := utils.GetValidators(sdk, filePath)
	if err != nil {
		return nil, err
	}
	prep, err := utils.GetPreparators(sdk, filePath)
	if err != nil {
		return nil, err
	}

	compileBuilder := executors.NewExecutorBuilder().
		WithValidator().
		WithSdkValidators(val).
		WithPreparator().
		WithSdkPreparators(prep).
		WithCompiler()

	compileBuilder = compileBuilder.
		WithCommand(executorConfig.CompileCmd).
		WithArgs(executorConfig.CompileArgs).
		WithFileName(filePath).
		WithWorkingDir(filesFolderPath)

	return compileBuilder, nil
}
