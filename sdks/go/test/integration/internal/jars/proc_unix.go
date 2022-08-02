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
	"golang.org/x/sys/unix"
	"os"
)

// UnixProcess wraps os.Process and changes the Kill function to perform a more
// graceful shutdown, mainly for compatibility with the timeout command.
type UnixProcess struct {
	proc *os.Process // The os.Process for the running jar.
}

// Kill gracefully shuts down the process. It is recommended to use this
// instead of directly killing the process.
func (p *UnixProcess) Kill() error {
	// Avoid using SIGKILL. If the jar is wrapped in the timeout command
	// SIGKILL will kill the timeout and leave the jar running.
	return p.proc.Signal(unix.SIGTERM)
}
