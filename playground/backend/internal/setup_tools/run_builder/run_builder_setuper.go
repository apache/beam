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

package run_builder

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/executors"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"beam.apache.org/playground/backend/internal/logger"
	"fmt"
	"github.com/google/uuid"
)

// Setup returns executors.RunBuilder setup it according to sdk
func Setup(pipelineId uuid.UUID, lc *fs_tool.LifeCycle, workingDir string, sdkEnv *environment.BeamEnvs, compileBuilder *executors.CompileBuilder) (*executors.RunBuilder, error) {
	runBuilder := compileBuilder.
		WithRunner().
		WithCommand(sdkEnv.ExecutorConfig.RunCmd).
		WithArgs(sdkEnv.ExecutorConfig.RunArgs).
		WithWorkingDir(lc.GetAbsoluteExecutableFilesFolderPath())

	switch sdkEnv.ApacheBeamSdk {
	case pb.Sdk_SDK_JAVA:
		className, err := lc.ExecutableName(pipelineId, workingDir)
		if err != nil {
			logger.Errorf("%s: get executable file name: %s\n", pipelineId, err.Error())
			return nil, err
		}

		runBuilder = runBuilder.
			WithClassName(className)
	case pb.Sdk_SDK_GO:
		runBuilder = runBuilder.WithCommand(lc.GetAbsoluteBinaryFilePath())
	default:
		return nil, fmt.Errorf("incorrect sdk: %s", sdkEnv.ApacheBeamSdk)
	}
	return runBuilder, nil
}
