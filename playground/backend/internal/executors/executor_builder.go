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
	"beam.apache.org/playground/backend/internal/validators"
)

type handler func(executor *Executor)

//ExecutorBuilder struct
type ExecutorBuilder struct {
	actions []handler
}

//CompileBuilder facet of ExecutorBuilder
type CompileBuilder struct {
	ExecutorBuilder
}

//RunBuilder facet of ExecutorBuilder
type RunBuilder struct {
	ExecutorBuilder
}

//ValidatorBuilder facet of ExecutorBuilder
type ValidatorBuilder struct {
	ExecutorBuilder
}

//NewExecutorBuilder constructor for Executor
func NewExecutorBuilder() *ExecutorBuilder {
	return &ExecutorBuilder{}
}

// WithCompiler - Lives chains to type *ExecutorBuilder and returns a *CompileBuilder
func (b *ExecutorBuilder) WithCompiler() *CompileBuilder {
	return &CompileBuilder{*b}
}

// WithRunner - Lives chains to type *ExecutorBuilder and returns a *CompileBuilder
func (b *ExecutorBuilder) WithRunner() *RunBuilder {
	return &RunBuilder{*b}
}

// WithValidator - Lives chains to type *ExecutorBuilder and returns a *CompileBuilder
func (b *ExecutorBuilder) WithValidator() *ValidatorBuilder {
	return &ValidatorBuilder{*b}
}

//WithCommand adds compile command to executor
func (b *CompileBuilder) WithCommand(compileCmd string) *CompileBuilder {
	b.actions = append(b.actions, func(e *Executor) {
		e.compileArgs.commandName = compileCmd
	})
	return b
}

//WithArgs adds compile args to executor
func (b *CompileBuilder) WithArgs(compileArgs []string) *CompileBuilder {
	b.actions = append(b.actions, func(e *Executor) {
		e.compileArgs.commandArgs = compileArgs
	})
	return b
}

//WithFileName adds file name to executor
func (b *CompileBuilder) WithFileName(fileName string) *CompileBuilder {
	b.actions = append(b.actions, func(e *Executor) {
		e.compileArgs.fileName = fileName
	})
	return b
}

//WithWorkingDir adds dir path to executor
func (b *CompileBuilder) WithWorkingDir(dir string) *CompileBuilder {
	b.actions = append(b.actions, func(e *Executor) {
		e.compileArgs.workingDir = dir
	})
	return b
}

//WithCommand adds run command to executor
func (b *RunBuilder) WithCommand(runCmd string) *RunBuilder {
	b.actions = append(b.actions, func(e *Executor) {
		e.runArgs.commandName = runCmd
	})
	return b
}

//WithArgs adds run args to executor
func (b *RunBuilder) WithArgs(runArgs []string) *RunBuilder {
	b.actions = append(b.actions, func(e *Executor) {
		e.runArgs.commandArgs = runArgs
	})
	return b
}

//WithClassName adds file name to executor
func (b *RunBuilder) WithClassName(name string) *RunBuilder {
	b.actions = append(b.actions, func(e *Executor) {
		e.runArgs.fileName = name
	})
	return b
}

//WithGraphOutput adds the need of graph output to executor
func (b *RunBuilder) WithGraphOutput() *RunBuilder {
	b.actions = append(b.actions, func(e *Executor) {
		//todo
	})
	return b
}

//WithWorkingDir adds dir path to executor
func (b *RunBuilder) WithWorkingDir(dir string) *RunBuilder {
	b.actions = append(b.actions, func(e *Executor) {
		e.runArgs.workingDir = dir
	})
	return b
}

//WithSdkValidators sets validators to executor
func (b *ValidatorBuilder) WithSdkValidators(validators *[]validators.Validator) *ValidatorBuilder {
	b.actions = append(b.actions, func(e *Executor) {
		e.validators = *validators
	})
	return b
}

//Build builds the executor object
func (b *ExecutorBuilder) Build() Executor {
	executor := Executor{}
	for _, a := range b.actions {
		a(&executor)
	}
	return executor
}
