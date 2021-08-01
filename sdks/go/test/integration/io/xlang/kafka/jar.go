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
	"net"
	"os"
	"os/exec"
	"strconv"
	"time"
)

// kafkaCluster contains anything needed to use and clean up the Kafka cluster
// once it's been started.
type kafkaCluster struct {
	proc          *os.Process // The process information for the running jar.
	bootstrapAddr string      // The bootstrap address to connect to Kafka.
}

// runLocalKafka takes a Kafka jar filepath and runs a local Kafka cluster,
// returning the bootstrap server for that cluster.
func runLocalKafka(jar string) (*kafkaCluster, error) {
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

	cmd := exec.Command("java", "-jar", jar, kafkaPort, zookeeperPort)
	err = cmd.Start()
	if err != nil {
		return nil, err
	}
	time.Sleep(3 * time.Second) // Wait a bit for the cluster to start.

	return &kafkaCluster{proc: cmd.Process, bootstrapAddr: "localhost:" + kafkaPort}, nil
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
