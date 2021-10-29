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
	"beam.apache.org/playground/backend/internal/validators"
	"context"
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
}

// Validate return the function that apply all validators of executor
func (ex *Executor) Validate() func(chan bool, chan error) {
	return func(doneCh chan bool, errCh chan error) {
		for _, validator := range ex.validators {
			err := validator.Validator(validator.Args...)
			if err != nil {
				errCh <- err
				doneCh <- false
				return
			}
		}
		doneCh <- true
		return
	}
}

// Compile prepares the Cmd for code compilation
// Returns Cmd instance
func (ex *Executor) Compile(ctx context.Context) *exec.Cmd {
	args := append(ex.compileArgs.commandArgs, ex.compileArgs.fileName)
	cmd := exec.CommandContext(ctx, ex.compileArgs.commandName, args...)
	cmd.Dir = ex.compileArgs.workingDir
	return cmd
}

// Run prepares the Cmd for execution of the code
// Returns Cmd instance
func (ex *Executor) Run(ctx context.Context) *exec.Cmd {
	args := append(ex.runArgs.commandArgs, ex.runArgs.fileName)
	cmd := exec.CommandContext(ctx, ex.runArgs.commandName, args...)
	cmd.Dir = ex.runArgs.workingDir
	return cmd
}
