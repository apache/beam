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

package kafka

import (
	"os"
)

// kafkaCluster contains anything needed to use and clean up the Kafka cluster
// once it's been started.
type cluster struct {
	proc          *os.Process // The process information for the running jar.
	bootstrapAddr string      // The bootstrap address to connect to Kafka.
}

// Shutdown gracefully shuts down the cluster. It is recommended to use this
// instead of directly killing the process.
func (kc *cluster) Shutdown() {
	// Avoid using SIGKILL. The cluster is wrapped in the timeout command,
	// so SIGKILL will kill the timeout and leave the cluster running.
	kc.proc.Kill()
}
