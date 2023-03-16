package executors

import (
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"beam.apache.org/playground/backend/internal/utils"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	javaLogConfigFileName        = "logging.properties"
	javaLogConfigFilePlaceholder = "{logConfigFile}"
)

func getJavaCompileCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, executorConfig *environment.ExecutorConfig) (*exec.Cmd, error) {
	compileCmd := executorConfig.CompileCmd
	workingDir := paths.AbsoluteBaseFolderPath
	args := executorConfig.CompileArgs

	javaSources, err := GetFilesFromFolder(paths.AbsoluteSourceFileFolderPath, fs_tool.JavaSourceFileExtension)
	if err != nil {
		return nil, err
	}
	args = append(args, javaSources...)

	cmd := exec.CommandContext(ctx, compileCmd, args...)
	cmd.Dir = workingDir
	return cmd, nil
}

func getJavaRunCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, pipelineOptions string, executorConfig *environment.ExecutorConfig) (*exec.Cmd, error) {
	workingDir := paths.AbsoluteBaseFolderPath

	pipelineOptions = utils.ReplaceSpacesWithEquals(pipelineOptions)
	args := replaceLogPlaceholder(paths, executorConfig)

	className, err := paths.FindExecutableName(ctx, paths.AbsoluteExecutableFileFolderPath)
	if err != nil {
		return nil, fmt.Errorf("no executable file name found for JAVA pipeline at %s: %s", paths.AbsoluteExecutableFileFolderPath, err)
	}
	pipelineOptionsSplit := strings.Split(pipelineOptions, " ")

	args = append(args, className)
	args = append(args, pipelineOptionsSplit...)

	cmd := exec.CommandContext(ctx, executorConfig.RunCmd, args...)
	cmd.Dir = workingDir

	return cmd, err
}

func getJavaRunTestCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, executorConfig *environment.ExecutorConfig) (*exec.Cmd, error) {
	workingDir := paths.AbsoluteBaseFolderPath
	testCmd := executorConfig.TestCmd
	args := executorConfig.TestArgs

	className, err := paths.FindTestExecutableName(ctx, paths.AbsoluteExecutableFileFolderPath)
	if err != nil {
		return nil, fmt.Errorf("no executable file name found for JAVA pipeline at %s: %s", paths.AbsoluteExecutableFileFolderPath, err)
	}
	args = append(args, className)

	cmd := exec.CommandContext(ctx, testCmd, args...)
	cmd.Dir = workingDir
	return cmd, nil
}

// ReplaceLogPlaceholder replaces placeholder for log for JAVA SDK
func replaceLogPlaceholder(paths *fs_tool.LifeCyclePaths, executorConfig *environment.ExecutorConfig) []string {
	args := make([]string, 0)
	for _, arg := range executorConfig.RunArgs {
		if strings.Contains(arg, javaLogConfigFilePlaceholder) {
			logConfigFilePath := filepath.Join(paths.AbsoluteBaseFolderPath, javaLogConfigFileName)
			arg = strings.Replace(arg, javaLogConfigFilePlaceholder, logConfigFilePath, 1)
		}
		args = append(args, arg)
	}
	return args
}
