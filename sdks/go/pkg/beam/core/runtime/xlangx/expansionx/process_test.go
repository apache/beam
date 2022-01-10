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

package expansionx

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestFindOpenPort(t *testing.T) {
	port, err := findOpenPort()
	if err != nil {
		t.Fatalf("failed to find open port, got %v", port)
	}
	if port < 1 || port > 65535 {
		t.Errorf("port out of TCP range [1, 66535], got %d", port)
	}
}

func TestNewExpansionServiceRunner(t *testing.T) {
	testPath := "path/to/jar"
	testPort := "8097"
	serviceRunner, err := NewExpansionServiceRunner(testPath, testPort)
	if err != nil {
		t.Fatalf("NewExpansionServiceRunner failed, got %v", err)
	}
	if serviceRunner.jarPath != testPath {
		t.Errorf("JAR path mismatch: wanted %v, got %v", testPath, serviceRunner.jarPath)
	}
	if serviceRunner.servicePort != testPort {
		t.Errorf("service port mismatch: wanted %v, got %v", testPort, serviceRunner.servicePort)
	}
	commandString := strings.Join(serviceRunner.serviceCommand.Args, " ")
	if !strings.Contains(commandString, "java") {
		t.Errorf("command string missing java invocation, got %v", commandString)
	}
	if !strings.Contains(commandString, "-jar") {
		t.Errorf("command string missing -jar flag, got %v", commandString)
	}
	if !strings.Contains(commandString, testPath) {
		t.Errorf("command string missing test path: want %v, got %v", testPath, commandString)
	}
}

func TestGetPort(t *testing.T) {
	testPort := "8097"
	serviceRunner, err := NewExpansionServiceRunner("", testPort)
	if err != nil {
		t.Fatalf("NewExpansionServiceRunner failed, got %v", err)
	}
	observedPort := serviceRunner.GetPort()
	if observedPort != testPort {
		t.Errorf("GetPort() returned mismatched value: wanted %v, got %v", observedPort, testPort)
	}
}

func TestStartService_badCommand(t *testing.T) {
	serviceRunner, err := NewExpansionServiceRunner("", "")
	if err != nil {
		t.Fatalf("NewExpansionServiceRunner failed, got %v", err)
	}
	serviceRunner.serviceCommand = exec.Command("jahva", "-jar")
	err = serviceRunner.StartService()
	if err == nil {
		t.Error("StartService succeeded when it should have failed")
	}
}

func TestStartService_good(t *testing.T) {
	serviceRunner, err := NewExpansionServiceRunner("", "")
	if err != nil {
		t.Fatalf("NewExpansionServiceRunner failed, got %v", err)
	}
	serviceRunner.serviceCommand = exec.Command("which", "go")
	err = serviceRunner.StartService()
	if err != nil {
		t.Errorf("StartService failed when it should have succeeded, got %v", err)
	}
}

func makeTestServiceRunner() *ExpansionServiceRunner {
	return &ExpansionServiceRunner{jarPath: "", serviceCommand: &exec.Cmd{}}
}

func TestStopService_NoStart(t *testing.T) {
	processStruct := makeTestServiceRunner()
	err := processStruct.StopService()
	if err == nil {
		t.Errorf("StopService succeeded when it should have failed")
	}
}

func TestStopService_AlreadyStopped(t *testing.T) {
	processStruct := makeTestServiceRunner()
	// Set ProcessState to non-nil, indicating that the process exited
	processStruct.serviceCommand.ProcessState = &os.ProcessState{}
	err := processStruct.StopService()
	if err == nil {
		t.Errorf("StopSercvice succeeded when it should have failed")
	}
}
