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
	"beam.apache.org/playground/backend/pkg/fs_tool"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	beamJarPath   = "/opt/apache/beam/jars/beam-sdks-java-harness.jar"
	runnerJarPath = "/opt/apache/beam/jars/beam-runners-direct.jar"
	slf4jPath     = "/opt/apache/beam/jars/slf4j-jdk14.jar"
	javaExtension = ".java"
)

type CompileError struct {
	error string
}

func (e *CompileError) Error() string {
	return fmt.Sprintf("Compilation error: %v", e.error)
}

// JavaExecutor for Java code
type JavaExecutor struct {
	fs fs_tool.JavaFileSystemService
}

func (javaExec JavaExecutor) Validate(fileName string) (bool, error) {
	filePath := filepath.Join(javaExec.fs.GetSrcPath(), fileName)
	return fs_tool.CheckPathIsValid(filePath, javaExtension)
}

func (javaExec JavaExecutor) Compile(fileName string) error {
	cmd := exec.Command("javac", "-d", javaExec.fs.GetBinPath(), "-classpath", beamJarPath,
		filepath.Join(javaExec.fs.GetSrcPath(), fileName))
	out, err := cmd.CombinedOutput()
	if err != nil {
		return &CompileError{string(out)}
	}
	return nil
}

func (javaExec JavaExecutor) Run(className string) (string, error) {
	fullClassPath := strings.Join([]string{javaExec.fs.GetBinPath(), beamJarPath, runnerJarPath, slf4jPath}, ":")
	cmd := exec.Command("java", "-cp", fullClassPath, className)
	out, err := cmd.Output()
	return string(out), err
}

// NewJavaExecutor creates new instance of java executor
func NewJavaExecutor(fs *fs_tool.JavaFileSystemService) *JavaExecutor {
	return &JavaExecutor{*fs}
}
