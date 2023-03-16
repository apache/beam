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

package executors

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
)

// GetCompileCmd prepares the Cmd for code compilation
// Returns Cmd instance
func GetCompileCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, sdkEnv *environment.BeamEnvs) (*exec.Cmd, error) {
	switch sdkEnv.ApacheBeamSdk {
	case pb.Sdk_SDK_JAVA:
		return getJavaCompileCmd(ctx, paths, sdkEnv.ExecutorConfig)
	case pb.Sdk_SDK_GO:
		return getGoCompileCmd(ctx, paths, sdkEnv.ExecutorConfig)
	case pb.Sdk_SDK_PYTHON:
		return getPythonCompileCmd(ctx, paths, sdkEnv.ExecutorConfig)
	case pb.Sdk_SDK_SCIO:
		return getScioCompileCmd(ctx, paths, sdkEnv.ExecutorConfig)
	}

	return nil, fmt.Errorf("unsupported sdk '%s'", sdkEnv.ApacheBeamSdk.String())
}

// GetRunCmd prepares the Cmd for execution of the code
// Returns Cmd instance
func GetRunCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, pipelineOptions string, sdkEnv *environment.BeamEnvs) (*exec.Cmd, error) {
	switch sdkEnv.ApacheBeamSdk {
	case pb.Sdk_SDK_JAVA:
		return getJavaRunCmd(ctx, paths, pipelineOptions, sdkEnv.ExecutorConfig)
	case pb.Sdk_SDK_GO:
		return getGoRunCmd(ctx, paths, pipelineOptions, sdkEnv.ExecutorConfig)
	case pb.Sdk_SDK_PYTHON:
		return getPythonRunCmd(ctx, paths, pipelineOptions, sdkEnv.ExecutorConfig)
	case pb.Sdk_SDK_SCIO:
		return getScioRunCmd(ctx, paths, pipelineOptions, sdkEnv.ExecutorConfig)
	}

	return nil, fmt.Errorf("unsupported sdk '%s'", sdkEnv.ApacheBeamSdk.String())
}

func GetRunTest(ctx context.Context, paths *fs_tool.LifeCyclePaths, sdkEnv *environment.BeamEnvs) (*exec.Cmd, error) {
	switch sdkEnv.ApacheBeamSdk {
	case pb.Sdk_SDK_JAVA:
		return getJavaRunTestCmd(ctx, paths, sdkEnv.ExecutorConfig)
	case pb.Sdk_SDK_GO:
		return getGoRunTestCmd(ctx, paths, sdkEnv.ExecutorConfig)
	case pb.Sdk_SDK_PYTHON:
		return getPythonRunTestCmd(ctx, paths, sdkEnv.ExecutorConfig)
	case pb.Sdk_SDK_SCIO:
		return getScioRunTestCmd(ctx, paths, sdkEnv.ExecutorConfig)
	}

	return nil, fmt.Errorf("unsupported sdk '%s'", sdkEnv.ApacheBeamSdk.String())
}

// GetFirstFileFromFolder return a name of the first file in a specified folder
func GetFilesFromFolder(folderAbsolutePath string, extension string) ([]string, error) {
	return filepath.Glob(fmt.Sprintf("%s/*%s", folderAbsolutePath, extension))
}
