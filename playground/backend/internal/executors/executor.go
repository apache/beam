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

type filePath string

// Executor interface for all executors (Java/Python/Go/SCIO)
type Executor struct {
	fileName    string
	dirPath     string
	validators  []func(filePath) error
	compileName string
	compileArgs []string
	runName     string
	runArgs     []string
}

// Validate checks that the file exists and that extension of the file matches the SDK.
// Return result of validation (true/false) and error if it occurs
func (*Executor) Validate(fileName string) error {
	return nil
}

// Compile compiles the code and creates executable file.
// Return error if it occurs
func (*Executor) Compile(fileName string) error {
	return nil
}

// Run runs the executable file.
// Return logs and error if it occurs
func (*Executor) Run(fileName string) (string, error) {
	return "", nil
}
