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
	"beam.apache.org/playground/backend/internal/preparators"
	"beam.apache.org/playground/backend/internal/validators"
	"os/exec"
)

//CmdConfiguration for base cmd code execution
type CmdConfiguration struct {
	fileName    string
	workingDir  string
	commandName string
	commandArgs []string
}

// Executor struct for all executors (Java/Python/Go/SCIO)
type Executor struct {
	compileArgs CmdConfiguration
	runArgs     CmdConfiguration
	validators  []validators.Validator
	preparators []preparators.Preparator
}

// Validate returns the function that applies all validators of executor
func (ex *Executor) Validate() func() error {
	return func() error {
		for _, validator := range ex.validators {
			err := validator.Validator(validator.Args...)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

// Prepare returns the function that applies all preparations of executor
func (ex *Executor) Prepare() func() error {
	return func() error {
		for _, preparator := range ex.preparators {
			err := preparator.Prepare(preparator.Args...)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

// Compile prepares the Cmd for code compilation
// Returns Cmd instance
func (ex *Executor) Compile() *exec.Cmd {
	args := append(ex.compileArgs.commandArgs, ex.compileArgs.fileName)
	cmd := exec.Command(ex.compileArgs.commandName, args...)
	cmd.Dir = ex.compileArgs.workingDir
	return cmd
}

// Run prepares the Cmd for execution of the code
// Returns Cmd instance
func (ex *Executor) Run() *exec.Cmd {
	args := append(ex.runArgs.commandArgs, ex.runArgs.fileName)
	cmd := exec.Command(ex.runArgs.commandName, args...)
	cmd.Dir = ex.runArgs.workingDir
	return cmd
}
