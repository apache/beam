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

type handler func(executor *Executor)

// ExecutorBuilder struct
type ExecutorBuilder struct {
	actions []handler
}

// RunBuilder facet of ExecutorBuilder
type RunBuilder struct {
	ExecutorBuilder
}

// ValidatorBuilder facet of ExecutorBuilder
type ValidatorBuilder struct {
	ExecutorBuilder
}

// PreparerBuilder facet of ExecutorBuilder
type PreparerBuilder struct {
	ExecutorBuilder
}

// UnitTestExecutorBuilder facet of ExecutorBuilder
type UnitTestExecutorBuilder struct {
	ExecutorBuilder
}

// NewExecutorBuilder constructor for Executor
func NewExecutorBuilder() *ExecutorBuilder {
	return &ExecutorBuilder{}
}

// WithRunner - Lives chains to type *ExecutorBuilder and returns a *CompileBuilder
func (b *ExecutorBuilder) WithRunner() *RunBuilder {
	return &RunBuilder{*b}
}

// WithTestRunner - Lives chains to type *ExecutorBuilder and returns a *UnitTestExecutorBuilder
func (b *ExecutorBuilder) WithTestRunner() *UnitTestExecutorBuilder {
	return &UnitTestExecutorBuilder{*b}
}

// WithExecutableFileNames adds file name to executor
func (b *RunBuilder) WithExecutableFileNames(names ...string) *RunBuilder {
	b.actions = append(b.actions, func(e *Executor) {
		e.runArgs.fileNames = names
	})
	return b
}

// WithWorkingDir adds dir path to executor
func (b *RunBuilder) WithWorkingDir(dir string) *RunBuilder {
	b.actions = append(b.actions, func(e *Executor) {
		e.runArgs.workingDir = dir
	})
	return b
}

// WithCommand adds run command to executor
func (b *RunBuilder) WithCommand(runCmd string) *RunBuilder {
	b.actions = append(b.actions, func(e *Executor) {
		e.runArgs.commandName = runCmd
	})
	return b
}

// WithArgs adds run args to executor
func (b *RunBuilder) WithArgs(runArgs []string) *RunBuilder {
	b.actions = append(b.actions, func(e *Executor) {
		e.runArgs.commandArgs = runArgs
	})
	return b
}

// WithCommand adds test command to executor
func (b *UnitTestExecutorBuilder) WithCommand(testCmd string) *UnitTestExecutorBuilder {
	b.actions = append(b.actions, func(e *Executor) {
		e.testArgs.commandName = testCmd
	})
	return b
}

// WithArgs adds test args to executor
func (b *UnitTestExecutorBuilder) WithArgs(testArgs []string) *UnitTestExecutorBuilder {
	b.actions = append(b.actions, func(e *Executor) {
		e.testArgs.commandArgs = testArgs
	})
	return b
}

// WithWorkingDir adds dir path to executor
func (b *UnitTestExecutorBuilder) WithWorkingDir(dir string) *UnitTestExecutorBuilder {
	b.actions = append(b.actions, func(e *Executor) {
		e.testArgs.workingDir = dir
	})
	return b
}

// WithExecutableFileName adds file name to executor
func (b *UnitTestExecutorBuilder) WithExecutableFileName(name string) *UnitTestExecutorBuilder {
	b.actions = append(b.actions, func(e *Executor) {
		e.testArgs.fileNames = append(e.testArgs.fileNames, name)
	})
	return b
}

// WithPipelineOptions adds pipeline options to executor
func (b *RunBuilder) WithPipelineOptions(pipelineOptions []string) *RunBuilder {
	b.actions = append(b.actions, func(e *Executor) {
		e.runArgs.pipelineOptions = pipelineOptions
	})
	return b
}

// Build builds the executor object
func (b *ExecutorBuilder) Build() Executor {
	executor := Executor{}
	for _, a := range b.actions {
		a(&executor)
	}
	return executor
}
