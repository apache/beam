package executors

import (
	"beam.apache.org/playground/backend/internal/fs_tool"
	"beam.apache.org/playground/backend/internal/utils"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	javaCompileCmd        = "javac"
	javaRunCmd            = "java"
	javaLoggConfigOption  = "-Djava.util.logging.config.file"
	javaLogConfigFileName = "logging.properties"
	javaTestCmd           = "java"
)

var javaCompileArgs = []string{"-d", "bin", "-parameters", "-classpath"}
var javaRunArgs = []string{"-cp", "bin:"}
var javaTestArgs = []string{"-cp", "bin:", "org.junit.runner.JUnitCore"}

func getJavaCompileCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths) (*exec.Cmd, error) {
	javaSources, err := getFilesFromFolder(paths.AbsoluteSourceFileFolderPath, fs_tool.JavaSourceFileExtension)
	if err != nil {
		return nil, err
	}

	cmd := exec.CommandContext(ctx, javaCompileCmd, append(javaCompileArgs, javaSources...)...)
	cmd.Dir = paths.AbsoluteBaseFolderPath
	return cmd, nil
}

func getJavaRunCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, pipelineOptions string) (*exec.Cmd, error) {
	pipelineOptions = utils.ReplaceSpacesWithEquals(pipelineOptions)

	logConfigFilePath := filepath.Join(paths.AbsoluteBaseFolderPath, javaLogConfigFileName)
	args := append(javaRunArgs, fmt.Sprintf("%s=%s", javaLoggConfigOption, logConfigFilePath))

	className, err := paths.FindExecutableName(ctx, paths.AbsoluteExecutableFileFolderPath)
	if err != nil {
		return nil, fmt.Errorf("no executable file name found for JAVA pipeline at %s: %s", paths.AbsoluteExecutableFileFolderPath, err)
	}
	pipelineOptionsSplit := strings.Split(pipelineOptions, " ")

	args = append(args, className)
	args = append(args, pipelineOptionsSplit...)

	cmd := exec.CommandContext(ctx, javaRunCmd, args...)
	cmd.Dir = paths.AbsoluteBaseFolderPath
	return cmd, nil
}

func getJavaRunTestCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths) (*exec.Cmd, error) {
	className, err := paths.FindTestExecutableName(ctx, paths.AbsoluteExecutableFileFolderPath)
	if err != nil {
		return nil, fmt.Errorf("no executable file name found for JAVA pipeline at %s: %s", paths.AbsoluteExecutableFileFolderPath, err)
	}

	cmd := exec.CommandContext(ctx, javaTestCmd, append(javaTestArgs, className)...)
	cmd.Dir = paths.AbsoluteBaseFolderPath
	return cmd, nil
}
