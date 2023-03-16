package executors

import (
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"context"
	"os/exec"
	"strings"
)

func getPythonCompileCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, executorConfig *environment.ExecutorConfig) (*exec.Cmd, error) {
	compileCmd := executorConfig.CompileCmd
	workingDir := paths.AbsoluteBaseFolderPath
	args := executorConfig.CompileArgs

	args = append(args, paths.AbsoluteSourceFilePath)

	cmd := exec.CommandContext(ctx, compileCmd, args...)
	cmd.Dir = workingDir
	return cmd, nil
}

func getPythonRunCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, pipelineOptions string, executorConfig *environment.ExecutorConfig) (*exec.Cmd, error) {
	workingDir := paths.AbsoluteBaseFolderPath

	pipelineOptionsSplit := strings.Split(pipelineOptions, " ")

	args := append(executorConfig.RunArgs, paths.AbsoluteExecutableFilePath)
	args = append(args, pipelineOptionsSplit...)

	cmd := exec.CommandContext(ctx, executorConfig.RunCmd, args...)
	cmd.Dir = workingDir
	return cmd, nil
}

func getPythonRunTestCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, executorConfig *environment.ExecutorConfig) (*exec.Cmd, error) {
	testCmd := executorConfig.TestCmd
	args := executorConfig.TestArgs
	workingDir := paths.AbsoluteSourceFileFolderPath

	args = append(args, paths.AbsoluteSourceFilePath)

	cmd := exec.CommandContext(ctx, testCmd, args...)
	cmd.Dir = workingDir
	return cmd, nil
}
