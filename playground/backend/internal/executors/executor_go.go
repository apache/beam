package executors

import (
	"beam.apache.org/playground/backend/internal/fs_tool"
	"context"
	"os/exec"
	"strings"
)

const (
	goCompileCmd = "go"
	goTestCmd    = "go"
)

var goCompileArgs = []string{"build", "-o", "bin"}
var goTestArgs = []string{"test", "-v"}

func getGoCompileCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths) (*exec.Cmd, error) {
	goSources, err := getFilesFromFolder(paths.AbsoluteSourceFileFolderPath, fs_tool.GoSourceFileExtension)
	if err != nil {
		return nil, err
	}
	args := append(goCompileArgs, goSources...)

	cmd := exec.CommandContext(ctx, goCompileCmd, args...)
	cmd.Dir = paths.AbsoluteBaseFolderPath
	return cmd, nil
}

func getGoRunCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, pipelineOptions string) (*exec.Cmd, error) {
	pipelineOptionsSplit := strings.Split(pipelineOptions, " ")

	cmd := exec.CommandContext(ctx, paths.AbsoluteExecutableFilePath, pipelineOptionsSplit...)
	cmd.Dir = paths.AbsoluteBaseFolderPath
	return cmd, nil
}

func getGoRunTestCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths) (*exec.Cmd, error) {
	cmd := exec.CommandContext(ctx, goTestCmd, append(goTestArgs, paths.AbsoluteSourceFileFolderPath)...)
	cmd.Dir = paths.AbsoluteSourceFileFolderPath
	return cmd, nil
}
