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
	"beam.apache.org/playground/backend/internal/utils"
	"context"
	"fmt"
	"os/exec"
)

const (
	scioRunCmd  = "sbt"
	scioRunArg  = "runMain"
	scioTestCmd = "sbt"
)

func getScioRunCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, pipelineOptions string) (*exec.Cmd, error) {
	className, err := paths.FindExecutableName(ctx, paths.AbsoluteBaseFolderPath)
	if err != nil {
		return nil, fmt.Errorf("no executable file name found for SCIO pipeline at %s: %s", paths.AbsoluteBaseFolderPath, err)
	}

	pipelineOptions = utils.ReplaceSpacesWithEquals(pipelineOptions)

	cmd := exec.CommandContext(ctx, scioRunCmd, fmt.Sprintf("%s %s %s", scioRunArg, className, pipelineOptions))
	cmd.Dir = paths.ProjectDir

	return cmd, nil
}

func getScioRunTestCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths) (*exec.Cmd, error) {
	cmd := exec.CommandContext(ctx, scioTestCmd, paths.AbsoluteSourceFilePath)
	cmd.Dir = paths.AbsoluteSourceFileFolderPath
	return cmd, nil
}
