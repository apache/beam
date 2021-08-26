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

// This file currently only has placeholder behavior. It is awaiting a
// Windows-specific implementation.
//go:build windows
// +build windows

package kafka

import (
	"errors"
	"os"
)

// kafkaCluster contains anything needed to use and clean up the Kafka cluster
// once it's been started.
type kafkaCluster struct {
	proc          *os.Process // The process information for the running jar.
	bootstrapAddr string      // The bootstrap address to connect to Kafka.
}

// Shutdown is currently a no-op.
func (kc *kafkaCluster) Shutdown() {}

// runLocalKafka is currently unimplemented on Windows.
func runLocalKafka(jar string, timeout string) (*kafkaCluster, error) {
	// This function requires a command which sets a timeout to the Kafka jar
	// execution, similar to the "timeout" command on Linux. Otherwise, the jar
	// does not get cleaned up if the test is killed (such as if it times out).
	return nil, errors.New("runLocalKafka does not support Windows")
}
