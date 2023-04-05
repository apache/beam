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

// Exclude build constraints of package golang.org/x/sys/unix.
//go:build !(aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris || zos)
// +build !aix,!darwin,!dragonfly,!freebsd,!linux,!netbsd,!openbsd,!solaris,!zos

package jars

import (
	"fmt"
	"os/exec"
	"runtime"
	"time"
)

// getTimeoutRunner is an OS-specific branch for determining what behavior to use for Run. This
// non-unix version does not handle timeout durations.
func getTimeoutRunner() runCallback {
	// Wrap run with error handling for OS that does not support timeout duration.
	return func(dur time.Duration, jar string, args ...string) (Process, error) {
		// Currently, we hard-fail here if a duration is provided but timeout is unsupported. If
		// we ever decide to soft-fail instead, this is the code to change.
		if dur != 0 {
			return nil, fmt.Errorf("cannot run jar: duration parameter provided but timeouts are unsupported on os %s", runtime.GOOS)
		}
		return run(jar, args...)
	}
}

// run simply starts up a jar and returns the cmd.Process.
func run(jar string, args ...string) (Process, error) {
	var cmdArr []string
	cmdArr = append(cmdArr, "java", "-jar", jar)
	cmdArr = append(cmdArr, args...)

	cmd := exec.Command(cmdArr[0], cmdArr[1:]...)
	err := cmd.Start()
	if err != nil {
		return nil, err
	}
	return cmd.Process, nil
}
