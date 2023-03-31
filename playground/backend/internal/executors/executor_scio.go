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
	"beam.apache.org/playground/backend/internal/utils"
	"context"
	"errors"
	"fmt"
	"os/exec"
)

const (
	scioRunCmd  = "sbt"
	scioRunArg  = "runMain"
	scioTestCmd = "sbt"
)

type scioExecutor struct {
	executor
}

func getScioExecutor(paths *fs_tool.LifeCyclePaths) Executor {
	return scioExecutor{executor{paths: paths, sdk: pb.Sdk_SDK_SCIO}}
}

func (e scioExecutor) GetCompileCmd(_ context.Context) (*exec.Cmd, error) {
	return nil, errors.New("compile step is not supported for scio")
}

func (e scioExecutor) GetRunCmd(ctx context.Context, pipelineOptions string) (*exec.Cmd, error) {
	className, err := e.paths.FindExecutableName(ctx)
	if err != nil {
		return nil, err
	}

	pipelineOptions = utils.ReplaceSpacesWithEquals(pipelineOptions)

	cmd := exec.CommandContext(ctx, scioRunCmd, fmt.Sprintf("%s %s %s", scioRunArg, className, pipelineOptions))
	cmd.Dir = e.paths.ProjectDir

	return cmd, nil
}

func (e scioExecutor) GetRunTestCmd(ctx context.Context) (*exec.Cmd, error) {
	cmd := exec.CommandContext(ctx, scioTestCmd, e.paths.AbsoluteSourceFilePath)
	cmd.Dir = e.paths.AbsoluteSourceFileFolderPath
	return cmd, nil
}
