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
	"beam.apache.org/playground/backend/internal/fs_tool"
	"context"
	"errors"
	"os/exec"
	"strings"
)

const (
	pythonRunCmd  = "python3"
	pythonTestCmd = "pytest"
)

type pythonExecutor struct {
	executor
}

func getPythonExecutor(paths *fs_tool.LifeCyclePaths) Executor {
	return pythonExecutor{
		executor: executor{
			paths: paths,
			sdk:   pb.Sdk_SDK_PYTHON,
		},
	}
}

func (e pythonExecutor) GetCompileCmd(_ context.Context) (*exec.Cmd, error) {
	return nil, errors.New("compile step is not supported for python")
}

func (e pythonExecutor) GetRunCmd(ctx context.Context, pipelineOptions string) (*exec.Cmd, error) {
	pipelineOptionsSplit := strings.Split(pipelineOptions, " ")

	args := append([]string{e.paths.AbsoluteExecutableFilePath}, pipelineOptionsSplit...)

	cmd := exec.CommandContext(ctx, pythonRunCmd, args...)
	cmd.Dir = e.paths.AbsoluteBaseFolderPath
	return cmd, nil
}

func (e pythonExecutor) GetRunTestCmd(ctx context.Context) (*exec.Cmd, error) {
	cmd := exec.CommandContext(ctx, pythonTestCmd, e.paths.AbsoluteSourceFilePath)
	cmd.Dir = e.paths.AbsoluteSourceFileFolderPath
	return cmd, nil
}
