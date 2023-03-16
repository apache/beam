package executors

import (
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"context"
	"os/exec"
	"strings"
)

func getGoCompileCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, executorConfig *environment.ExecutorConfig) (*exec.Cmd, error) {
	compileCmd := executorConfig.CompileCmd
	workingDir := paths.AbsoluteBaseFolderPath
	args := executorConfig.CompileArgs

	goSources, err := GetFilesFromFolder(paths.AbsoluteSourceFileFolderPath, fs_tool.GoSourceFileExtension)
	if err != nil {
		return nil, err
	}
	args = append(args, goSources...)

	cmd := exec.CommandContext(ctx, compileCmd, args...)
	cmd.Dir = workingDir
	return cmd, nil
}

func getGoRunCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, pipelineOptions string, executorConfig *environment.ExecutorConfig) (*exec.Cmd, error) {
	workingDir := paths.AbsoluteBaseFolderPath

	pipelineOptionsSplit := strings.Split(pipelineOptions, " ")

	cmd := exec.CommandContext(ctx, paths.AbsoluteExecutableFilePath, append(executorConfig.RunArgs, pipelineOptionsSplit...)...)
	cmd.Dir = workingDir
	return cmd, nil
}
func getGoRunTestCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, executorConfig *environment.ExecutorConfig) (*exec.Cmd, error) {
	testCmd := executorConfig.TestCmd
	args := executorConfig.TestArgs
	workingDir := paths.AbsoluteSourceFileFolderPath

	args = append(args, paths.AbsoluteSourceFileFolderPath)

	cmd := exec.CommandContext(ctx, testCmd, args...)
	cmd.Dir = workingDir
	return cmd, nil
}
