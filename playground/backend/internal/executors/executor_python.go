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
