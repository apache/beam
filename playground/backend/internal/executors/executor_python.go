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
	"beam.apache.org/playground/backend/internal/fs_tool"
	"context"
	"os/exec"
	"strings"
)

const (
	pythonRunCmd  = "python3"
	pythonTestCmd = "pytest"
)

func getPythonRunCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, pipelineOptions string) (*exec.Cmd, error) {
	pipelineOptionsSplit := strings.Split(pipelineOptions, " ")

	args := append([]string{paths.AbsoluteExecutableFilePath}, pipelineOptionsSplit...)

	cmd := exec.CommandContext(ctx, pythonRunCmd, args...)
	cmd.Dir = paths.AbsoluteBaseFolderPath
	return cmd, nil
}

func getPythonRunTestCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths) (*exec.Cmd, error) {
	cmd := exec.CommandContext(ctx, pythonTestCmd, paths.AbsoluteSourceFilePath)
	cmd.Dir = paths.AbsoluteSourceFileFolderPath
	return cmd, nil
}
