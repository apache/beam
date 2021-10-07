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

// Package executors
package executors

import (
	"beam.apache.org/playground/backend/internal/fs_tool"
	"os"
	"path/filepath"
	"strings"
)

const (
	beamJarPath    = "/opt/apache/beam/jars/beam-sdks-java-harness.jar"
	runnerJarPath  = "/opt/apache/beam/jars/beam-runners-direct.jar"
	slf4jPath      = "/opt/apache/beam/jars/slf4j-jdk14.jar"
	javaExtension  = ".java"
	javaCompileCmd = "javac"
	javaRunCmd     = "java"
	binFolder      = "bin"
)

// NewJavaExecutor creates an executor with Go specifics
func NewJavaExecutor(fs *fs_tool.LifeCycle, javaValidators *[]validatorWithArgs) *Executor {
	compileArgs := []string{"-d", binFolder, "-classpath", beamJarPath}
	fullClassPath := strings.Join([]string{binFolder, beamJarPath, runnerJarPath, slf4jPath}, ":")
	runArgs := []string{"-cp", fullClassPath}
	if javaValidators == nil {
		v := make([]validatorWithArgs, 0)
		javaValidators = &v
	}
	path, _ := os.Getwd()

	exec := new(Executor)
	exec.validators = *javaValidators
	exec.relativeFilePath = fs.GetRelativeExecutableFilePath()
	exec.absoulteFilePath = fs.GetAbsoluteExecutableFilePath()
	exec.dirPath = filepath.Join(path, fs.Folder.BaseFolder)
	exec.compileName = javaCompileCmd
	exec.runName = javaRunCmd
	exec.compileArgs = compileArgs
	exec.runArgs = runArgs
	return exec
}

// GetJavaValidators return validators methods that needed for Java file
func GetJavaValidators() *[]validatorWithArgs {
	validatorArgs := make([]interface{}, 1)
	validatorArgs[0] = javaExtension
	pathCheckerValidator := validatorWithArgs{
		validator: fs_tool.CheckPathIsValid,
		args:      validatorArgs,
	}
	validators := []validatorWithArgs{pathCheckerValidator}
	return &validators
}
