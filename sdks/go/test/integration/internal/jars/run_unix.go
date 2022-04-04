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

// Match build constraints of imported package golang.org/x/sys/unix.
//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris || zos
// +build aix darwin dragonfly freebsd linux netbsd openbsd solaris zos

package jars

import (
	"fmt"
	"os/exec"
	"time"
)

// getTimeoutRunner is an OS-specific branch for determining what behavior to use for Run. This
// Unix specific version handles timeout.
func getTimeoutRunner() runCallback {
	_, err := exec.LookPath("timeout")
	if err != nil {
		// Wrap run with Unix-specific error handling for missing timeout command.
		return func(dur time.Duration, jar string, args ...string) (Process, error) {
			// Currently, we hard-fail here if a duration is provided but timeout is unsupported. If
			// we ever decide to soft-fail instead, this is the code to change.
			if dur != 0 {
				return nil, fmt.Errorf("cannot run jar: duration parameter provided but 'timeout' command not installed: %w", err)
			}
			return run(dur, jar, args...)
		}
	}

	// Path for a supported timeout, just use the default run function.
	return run
}

// run starts up a jar, and wraps it in "timeout" only if a duration is provided. Processes are
// returned wrapped as Unix processes that provide graceful shutdown for unix specifically.
func run(dur time.Duration, jar string, args ...string) (Process, error) {
	var cmdArr []string
	if dur != 0 {
		durStr := fmt.Sprintf("%.2fm", dur.Minutes())
		cmdArr = append(cmdArr, "timeout", durStr)
	}
	cmdArr = append(cmdArr, "java", "-jar", jar)
	cmdArr = append(cmdArr, args...)

	cmd := exec.Command(cmdArr[0], cmdArr[1:]...)
	err := cmd.Start()
	if err != nil {
		return nil, err
	}
	return &UnixProcess{proc: cmd.Process}, nil
}
