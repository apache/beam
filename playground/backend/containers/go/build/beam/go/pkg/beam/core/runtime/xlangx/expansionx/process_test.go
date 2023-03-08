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
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestFindOpenPort(t *testing.T) {
	port, err := findOpenPort()
	if err != nil {
		t.Fatalf("failed to find open port, got %v", err)
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
	if got, want := serviceRunner.execPath, testPath; got != want {
		t.Errorf("got JAR path %v, want %v", got, want)
	}
	if got, want := serviceRunner.servicePort, testPort; got != want {
		t.Errorf("got service port %v, want %v", got, want)
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

func TestEndpoint(t *testing.T) {
	testPort := "8097"
	serviceRunner, err := NewExpansionServiceRunner("", testPort)
	if err != nil {
		t.Fatalf("NewExpansionServiceRunner failed, got %v", err)
	}
	want := "localhost:" + testPort
	if got := serviceRunner.Endpoint(); got != want {
		t.Errorf("got port %v, want %v", got, want)
	}
}

func makeAndArrangeMockGRPCService(t *testing.T, runner *ExpansionServiceRunner) error {
	server := grpc.NewServer()
	lis, err := net.Listen("tcp", runner.Endpoint())
	if err != nil {
		return err
	}

	go server.Serve(lis)
	t.Cleanup(func() { server.Stop(); lis.Close() })
	return nil
}

func TestPingEndpoint_bad(t *testing.T) {
	serviceRunner, err := NewExpansionServiceRunner("", "")
	if err != nil {
		t.Fatalf("NewExpansionServiceRunner failed, got %v", err)
	}
	err = serviceRunner.pingEndpoint(1 * time.Second)
	if err == nil {
		t.Errorf("pingEndpoint succeeded when it should have failed")
	}
}

func TestPingEndpoint_good(t *testing.T) {
	serviceRunner, err := NewExpansionServiceRunner("", "")
	if err != nil {
		t.Fatalf("NewExpansionServiceRunner failed, got %v", err)
	}
	err = makeAndArrangeMockGRPCService(t, serviceRunner)
	if err != nil {
		t.Fatalf("starting GRPC service failed, got %v", err)
	}
	err = serviceRunner.pingEndpoint(10 * time.Second)
	if err != nil {
		t.Errorf("pingEndpoint() failed, got %v", err)
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
	makeAndArrangeMockGRPCService(t, serviceRunner)
	// Drop in a command that shouldn't error on its own
	serviceRunner.serviceCommand = exec.Command("echo", "testing")
	err = serviceRunner.StartService()
	if err != nil {
		t.Errorf("StartService failed when it should have succeeded, got %v", err)
	}
}

func makeTestServiceRunner() *ExpansionServiceRunner {
	return &ExpansionServiceRunner{execPath: "", serviceCommand: &exec.Cmd{}}
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
