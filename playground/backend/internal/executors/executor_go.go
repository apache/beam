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
	"os/exec"
	"strings"
)

const (
	goCompileCmd = "go"
	goTestCmd    = "go"
)

type goExecutor struct {
	executor
}

var goCompileArgs = []string{"build", "-o", "bin"}
var goTestArgs = []string{"test", "-v"}

func getGoExecutor(paths *fs_tool.LifeCyclePaths) Executor {
	return &goExecutor{
		executor: executor{
			paths: paths,
			sdk:   pb.Sdk_SDK_GO,
		},
	}
}

func (e goExecutor) GetCompileCmd(ctx context.Context) (*exec.Cmd, error) {
	goSources, err := e.paths.GetSourceFiles()
	if err != nil {
		return nil, err
	}
	args := append(goCompileArgs, goSources...)

	cmd := exec.CommandContext(ctx, goCompileCmd, args...)
	cmd.Dir = e.paths.AbsoluteBaseFolderPath
	return cmd, nil
}

func (e goExecutor) GetRunCmd(ctx context.Context, pipelineOptions string) (*exec.Cmd, error) {
	pipelineOptionsSplit := strings.Split(pipelineOptions, " ")

	cmd := exec.CommandContext(ctx, e.paths.AbsoluteExecutableFilePath, pipelineOptionsSplit...)
	cmd.Dir = e.paths.AbsoluteBaseFolderPath
	return cmd, nil
}

func (e goExecutor) GetRunTestCmd(ctx context.Context) (*exec.Cmd, error) {
	cmd := exec.CommandContext(ctx, goTestCmd, append(goTestArgs, e.paths.AbsoluteSourceFileFolderPath)...)
	cmd.Dir = e.paths.AbsoluteSourceFileFolderPath
	return cmd, nil
}
