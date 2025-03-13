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

// Package execx contains wrappers and utilities for the exec package.
package execx

import (
	"io"
	"os"
	"os/exec"
)

// Execute runs the program with the given arguments. It attaches stdio to the
// child process.
func Execute(prog string, args ...string) error {
	return ExecuteEnvWithIO(nil, os.Stdin, os.Stdout, os.Stderr, prog, args...)
}

// ExecuteEnv runs the program with the given arguments with additional environment
// variables. It attaches stdio to the child process.
func ExecuteEnv(env map[string]string, prog string, args ...string) error {
	return ExecuteEnvWithIO(env, os.Stdin, os.Stdout, os.Stderr, prog, args...)
}

// ExecuteEnvWithIO runs the program with the given arguments with additional environment
// variables. It attaches custom IO to the child process.
func ExecuteEnvWithIO(env map[string]string, stdin io.Reader, stdout, stderr io.Writer, prog string, args ...string) error {
	cmd := exec.Command(prog, args...)
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if env != nil {
		cmd.Env = os.Environ()
		for k, v := range env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
	}

	return cmd.Run()
}
