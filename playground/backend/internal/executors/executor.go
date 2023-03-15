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
	pb "beam.apache.org/playground/backend/internal/api/v1"
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

// GetCompileCmd prepares the Cmd for code compilation
// Returns Cmd instance
func GetCompileCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, sdkEnv *environment.BeamEnvs) (*exec.Cmd, error) {
	sdk := sdkEnv.ApacheBeamSdk
	executorConfig := sdkEnv.ExecutorConfig

	compileCmd := executorConfig.CompileCmd
	workingDir := paths.AbsoluteBaseFolderPath
	args := executorConfig.CompileArgs

	switch sdk {
	case pb.Sdk_SDK_JAVA:
		javaSources, err := GetFilesFromFolder(paths.AbsoluteSourceFileFolderPath, fs_tool.JavaSourceFileExtension)
		if err != nil {
			return nil, err
		}
		args = append(args, javaSources...)
	case pb.Sdk_SDK_GO:
		goSources, err := GetFilesFromFolder(paths.AbsoluteSourceFileFolderPath, fs_tool.GoSourceFileExtension)
		if err != nil {
			return nil, err
		}
		args = append(args, goSources...)
	default:
		args = append(args, paths.AbsoluteSourceFilePath)
	}

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

func getGoRunCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, pipelineOptions string, executorConfig *environment.ExecutorConfig) (*exec.Cmd, error) {
	workingDir := paths.AbsoluteBaseFolderPath

	pipelineOptionsSplit := strings.Split(pipelineOptions, " ")

	cmd := exec.CommandContext(ctx, paths.AbsoluteExecutableFilePath, append(executorConfig.RunArgs, pipelineOptionsSplit...)...)
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

// GetRunCmd prepares the Cmd for execution of the code
// Returns Cmd instance
func GetRunCmd(ctx context.Context, paths *fs_tool.LifeCyclePaths, pipelineOptions string, sdkEnv *environment.BeamEnvs) (*exec.Cmd, error) {
	switch sdkEnv.ApacheBeamSdk {
	case pb.Sdk_SDK_JAVA:
		return getJavaRunCmd(ctx, paths, pipelineOptions, sdkEnv.ExecutorConfig)
	case pb.Sdk_SDK_GO:
		return getGoRunCmd(ctx, paths, pipelineOptions, sdkEnv.ExecutorConfig)
	case pb.Sdk_SDK_PYTHON:
		return getPythonRunCmd(ctx, paths, pipelineOptions, sdkEnv.ExecutorConfig)
	case pb.Sdk_SDK_SCIO:
		return getScioRunCmd(ctx, paths, pipelineOptions, sdkEnv.ExecutorConfig)
	}

	return nil, fmt.Errorf("unsupported sdk '%s'", sdkEnv.ApacheBeamSdk.String())
}

func GetRunTest(ctx context.Context, paths *fs_tool.LifeCyclePaths, sdkEnv *environment.BeamEnvs) (*exec.Cmd, error) {
	sdk := sdkEnv.ApacheBeamSdk
	executorConfig := sdkEnv.ExecutorConfig

	testCmd := executorConfig.TestCmd
	args := executorConfig.TestArgs
	workingDir := paths.AbsoluteSourceFileFolderPath

	switch sdk {
	case pb.Sdk_SDK_JAVA:
		className, err := paths.FindTestExecutableName(ctx, paths.AbsoluteExecutableFileFolderPath)
		if err != nil {
			return nil, fmt.Errorf("no executable file name found for JAVA pipeline at %s: %s", paths.AbsoluteExecutableFileFolderPath, err)
		}
		workingDir = paths.AbsoluteBaseFolderPath
		args = append(args, className)
	case pb.Sdk_SDK_GO:
		args = append(args, paths.AbsoluteSourceFileFolderPath)
	default:
		args = append(args, paths.AbsoluteSourceFilePath)
	}

	cmd := exec.CommandContext(ctx, testCmd, args...)
	cmd.Dir = workingDir
	return cmd, nil
}

// GetFirstFileFromFolder return a name of the first file in a specified folder
func GetFilesFromFolder(folderAbsolutePath string, extension string) ([]string, error) {
	return filepath.Glob(fmt.Sprintf("%s/*%s", folderAbsolutePath, extension))
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
