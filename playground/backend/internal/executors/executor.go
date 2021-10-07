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
	pb "beam.apache.org/playground/backend/internal/api"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"fmt"
	"os/exec"
)

type validatorWithArgs struct {
	validator func(filePath string, args ...interface{}) error
	args      []interface{}
}

// Executor interface for all executors (Java/Python/Go/SCIO)
type Executor struct {
	relativeFilePath string
	absoulteFilePath string
	dirPath          string
	executableDir    string
	validators       []validatorWithArgs
	compileName      string
	compileArgs      []string
	runName          string
	runArgs          []string
}

// Validate checks that the file exists and that extension of the file matches the SDK.
// Return result of validation (true/false) and error if it occurs
func (ex *Executor) Validate() error {
	for _, validator := range ex.validators {
		err := validator.validator(ex.absoulteFilePath, validator.args...)
		if err != nil {
			return err
		}
	}
	return nil
}

// Compile compiles the code and creates executable file.
// Return error if it occurs
func (ex *Executor) Compile() error {
	args := append(ex.compileArgs, ex.relativeFilePath)
	cmd := exec.Command(ex.compileName, args...)
	cmd.Dir = ex.dirPath
	s := cmd.String()
	fmt.Println(s)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return &CompileError{string(out)}
	}
	return nil
}

// Run runs the executable file.
// Return logs and error if it occurs
func (ex *Executor) Run(name string) (string, error) {
	args := append(ex.runArgs, name)
	cmd := exec.Command(ex.runName, args...)
	cmd.Dir = ex.dirPath
	out, err := cmd.Output()
	return string(out), err
}

// NewExecutor executes the compilation, running and validation of code
func NewExecutor(apacheBeamSdk pb.Sdk, fs *fs_tool.LifeCycle) (*Executor, error) {
	switch apacheBeamSdk {
	case pb.Sdk_SDK_JAVA:
		return NewJavaExecutor(fs, GetJavaValidators()), nil
	default:
		return nil, fmt.Errorf("%s isn't supported now", apacheBeamSdk)
	}
}

type CompileError struct {
	error string
}

func (e *CompileError) Error() string {
	return fmt.Sprintf("Compilation error: %v", e.error)
}
