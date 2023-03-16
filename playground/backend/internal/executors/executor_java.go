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
	javaCompileCmd        = "javac"
	javaRunCmd            = "java"
	javaLoggConfigOption  = "-Djava.util.logging.config.file"
	javaLogConfigFileName = "logging.properties"
	javaTestCmd           = "java"
)

func getJavaCompileArgs() ([]string, error) {
	classpath, err := environment.ConcatBeamJarsToString()
	if err != nil {
		return nil, fmt.Errorf("error during proccessing jars: %s", err.Error())
	}
	return []string{"-d", "bin", "-parameters", "-classpath", classpath}, nil
}

func getJavaRunArgs() ([]string, error) {
	classpath, err := environment.ConcatBeamJarsToString()
	if err != nil {
		return nil, fmt.Errorf("error during proccessing jars: %s", err.Error())
	}
	return []string{"-cp", fmt.Sprintf("bin:%s", classpath)}, nil
}

func getJavaTestArgs() ([]string, error) {
	classpath, err := environment.ConcatBeamJarsToString()
	if err != nil {
		return nil, fmt.Errorf("error during proccessing jars: %s", err.Error())
	}
	return []string{"-cp", fmt.Sprintf("bin:%s", classpath), "org.junit.runner.JUnitCore"}, nil
}

func getJavaCompileCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths) (*exec.Cmd, error) {
	javaSources, err := getFilesFromFolder(paths.AbsoluteSourceFileFolderPath, fs_tool.JavaSourceFileExtension)
	if err != nil {
		return nil, err
	}

	compileArgs, err := getJavaCompileArgs()
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, javaCompileCmd, append(compileArgs, javaSources...)...)
	cmd.Dir = paths.AbsoluteBaseFolderPath
	return cmd, nil
}

func getJavaRunCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, pipelineOptions string) (*exec.Cmd, error) {
	pipelineOptions = utils.ReplaceSpacesWithEquals(pipelineOptions)

	logConfigFilePath := filepath.Join(paths.AbsoluteBaseFolderPath, javaLogConfigFileName)
	runArgs, err := getJavaRunArgs()
	if err != nil {
		return nil, err
	}
	args := append(runArgs, fmt.Sprintf("%s=%s", javaLoggConfigOption, logConfigFilePath))

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

	testArgs, err := getJavaTestArgs()
	if err != nil {
		return nil, err
	}

	cmd := exec.CommandContext(ctx, javaTestCmd, append(testArgs, className)...)
	cmd.Dir = paths.AbsoluteBaseFolderPath
	return cmd, nil
}
