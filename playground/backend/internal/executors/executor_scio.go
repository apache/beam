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
