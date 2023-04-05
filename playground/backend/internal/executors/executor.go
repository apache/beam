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
	"beam.apache.org/playground/backend/internal/preparers"
	"beam.apache.org/playground/backend/internal/validators"
	"context"
	"os/exec"
	"sync"
)

type ExecutionType string

const (
	Run  ExecutionType = "Run"
	Test ExecutionType = "RunTest"
)

// CmdConfiguration for base cmd code execution
type CmdConfiguration struct {
	fileNames       []string
	workingDir      string
	commandName     string
	commandArgs     []string
	pipelineOptions []string
}

// Executor struct for all sdks (Java/Python/Go/SCIO)
type Executor struct {
	compileArgs CmdConfiguration
	runArgs     CmdConfiguration
	testArgs    CmdConfiguration
	validators  []validators.Validator
	preparers   []preparers.Preparer
}

// Validate returns the function that applies all validators of executor
func (ex *Executor) Validate() func(chan bool, chan error, *sync.Map) {
	return func(doneCh chan bool, errCh chan error, valRes *sync.Map) {
		validationErrors := make(chan error, len(ex.validators))
		var wg sync.WaitGroup
		for _, validator := range ex.validators {
			wg.Add(1)
			go func(validationErrors chan error, valRes *sync.Map, validator validators.Validator) {
				defer wg.Done()
				res, err := validator.Validator(validator.Args...)
				if err != nil {
					validationErrors <- err
				}
				valRes.Store(validator.Name, res)
			}(validationErrors, valRes, validator)
		}
		wg.Wait()
		select {
		case err := <-validationErrors:
			errCh <- err
			doneCh <- false
		default:
			doneCh <- true
		}
	}
}

// Prepare returns the function that applies all preparations of executor
func (ex *Executor) Prepare() func(chan bool, chan error, *sync.Map) {
	return func(doneCh chan bool, errCh chan error, validationResults *sync.Map) {
		for _, preparer := range ex.preparers {
			preparer.Args = append(preparer.Args, validationResults)
			err := preparer.Prepare(preparer.Args...)
			if err != nil {
				errCh <- err
				doneCh <- false
				return
			}
		}
		doneCh <- true
	}
}

// Compile prepares the Cmd for code compilation
// Returns Cmd instance
func (ex *Executor) Compile(ctx context.Context) *exec.Cmd {
	args := append(ex.compileArgs.commandArgs, ex.compileArgs.fileNames...)
	cmd := exec.CommandContext(ctx, ex.compileArgs.commandName, args...)
	cmd.Dir = ex.compileArgs.workingDir
	return cmd
}

// Run prepares the Cmd for execution of the code
// Returns Cmd instance
func (ex *Executor) Run(ctx context.Context) *exec.Cmd {
	args := ex.runArgs.commandArgs
	if len(ex.runArgs.fileNames) > 0 {
		args = append(args, ex.runArgs.fileNames...)
	}
	if ex.runArgs.pipelineOptions != nil && ex.runArgs.pipelineOptions[0] != "" {
		args = append(args, ex.runArgs.pipelineOptions...)
	}
	cmd := exec.CommandContext(ctx, ex.runArgs.commandName, args...)
	cmd.Dir = ex.runArgs.workingDir
	return cmd
}

// RunTest prepares the Cmd for execution of the unit test
// Returns Cmd instance
func (ex *Executor) RunTest(ctx context.Context) *exec.Cmd {
	args := append(ex.testArgs.commandArgs, ex.testArgs.fileNames...)
	cmd := exec.CommandContext(ctx, ex.testArgs.commandName, args...)
	cmd.Dir = ex.testArgs.workingDir
	return cmd
}
