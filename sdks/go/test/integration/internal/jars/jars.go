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

// Package jars contains functionality for running jars for integration tests. The main entry point
// for running a jar is the Run function. The Process interface is used to interact with the running
// jars, and most importantly for shutting down the jars once finished with them.
package jars

import (
	"fmt"
	"os/exec"
	"time"
)

type runCallback func(dur time.Duration, jar string, args ...string) (Process, error)

var runner runCallback // Saves which behavior to use when Run is called.

func init() {
	runner = getRunner()
}

// getRunner is used to determine the appropriate behavior for run during initialization time,
// based on the OS and installed binaries of the system. This is returned as a runCallback which
// can be called whenever Run is called. If an error prevents Run from being used at all (for
// example, Java is not installed), then the runCallback will return that error.
func getRunner() runCallback {
	// First check if we can even run jars.
	_, err := exec.LookPath("java")
	if err != nil {
		err := fmt.Errorf("cannot run jar: 'java' command not installed: %w", err)
		return func(_ time.Duration, _ string, _ ...string) (Process, error) {
			return nil, err
		}
	}

	// Defer to OS-specific logic for checking for presence of timeout command.
	return getTimeoutRunner()
}

// Run runs a jar given an optional duration, a path to the jar, and any desired arguments to the
// jar. It returns a Process object which can be used to shut down the jar once finished.
//
// The dur parameter is a duration for the timeout command which can be used to automatically kill
// the jar after a set duration, in order to avoid resource leakage. Timeout is described here:
// https://man7.org/linux/man-pages/man1/timeout.1.html. Durations will be translated from
// time.Duration to a string based on the number of minutes. If a duration is provided but the
// system is unable to use the timeout is unable to use the timeout command, this function will
// return an error. To indicate that a duration isn't needed, pass in 0.
func Run(dur time.Duration, jar string, args ...string) (Process, error) {
	return runner(dur, jar, args...)
}
