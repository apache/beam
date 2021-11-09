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

package kafka

import (
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"time"
)

// runLocalKafka takes a Kafka jar filepath and runs a local Kafka cluster with
// a timeout (via the timeout command), returning the bootstrap server. Requires
// an environment with the timeout command.
func runLocalKafka(jar string, timeout string) (*cluster, error) {
	_, err := exec.LookPath("java")
	if err != nil {
		return nil, err
	}
	_, err = exec.LookPath("timeout")
	if err != nil && len(timeout) != 0 {
		return nil, fmt.Errorf("\"timeout\" required for kafka_jar_timeout flag: %s", err)
	}

	port, err := getOpenPort()
	if err != nil {
		return nil, err
	}
	kafkaPort := strconv.Itoa(port)
	port, err = getOpenPort()
	if err != nil {
		return nil, err
	}
	zookeeperPort := strconv.Itoa(port)

	var cmdArr []string
	if len(timeout) != 0 {
		cmdArr = append(cmdArr, "timeout", timeout)
	}
	cmdArr = append(cmdArr, "java", "-jar", jar, kafkaPort, zookeeperPort)

	cmd := exec.Command(cmdArr[0], cmdArr[1:]...)
	err = cmd.Start()
	if err != nil {
		return nil, err
	}
	time.Sleep(3 * time.Second) // Wait a bit for the cluster to start.

	return &cluster{proc: cmd.Process, bootstrapAddr: "localhost:" + kafkaPort}, nil
}

// getOpenPort gets an open TCP port and returns it, or an error on failure.
func getOpenPort() (int, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}
