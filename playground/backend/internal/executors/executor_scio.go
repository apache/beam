package executors

import (
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"beam.apache.org/playground/backend/internal/utils"
	"context"
	"fmt"
	"os/exec"
)

func getScioCompileCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, executorConfig *environment.ExecutorConfig) (*exec.Cmd, error) {
	compileCmd := executorConfig.CompileCmd
	workingDir := paths.AbsoluteBaseFolderPath
	args := executorConfig.CompileArgs

	args = append(args, paths.AbsoluteSourceFilePath)

	cmd := exec.CommandContext(ctx, compileCmd, args...)
	cmd.Dir = workingDir
	return cmd, nil
}

func getScioRunCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, pipelineOptions string, executorConfig *environment.ExecutorConfig) (*exec.Cmd, error) {
	workingDir := paths.ProjectDir

	className, err := paths.FindExecutableName(ctx, paths.AbsoluteBaseFolderPath)
	if err != nil {
		return nil, fmt.Errorf("no executable file name found for SCIO pipeline at %s: %s", paths.AbsoluteBaseFolderPath, err)
	}

	pipelineOptions = utils.ReplaceSpacesWithEquals(pipelineOptions)
	stringArg := fmt.Sprintf("%s %s %s", executorConfig.RunArgs[0], className, pipelineOptions)

	cmd := exec.CommandContext(ctx, executorConfig.RunCmd, append(executorConfig.RunArgs, stringArg)...)
	cmd.Dir = workingDir

	return cmd, nil
}

func getScioRunTestCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, executorConfig *environment.ExecutorConfig) (*exec.Cmd, error) {
	testCmd := executorConfig.TestCmd
	args := executorConfig.TestArgs
	workingDir := paths.AbsoluteSourceFileFolderPath

	args = append(args, paths.AbsoluteSourceFilePath)

	cmd := exec.CommandContext(ctx, testCmd, args...)
	cmd.Dir = workingDir
	return cmd, nil
}
