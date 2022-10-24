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
	"context"
	"fmt"
	"net"
	"os/exec"
	"time"

	"google.golang.org/grpc"
)

// ExpansionServiceRunner is a type that holds information required to
// start up a Beam Expansion Service JAR and maintain a handle on the
// process running the service to enable shutdown as well.
type ExpansionServiceRunner struct {
	execPath       string
	servicePort    string
	serviceCommand *exec.Cmd
}

func findOpenPort() (int, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

// NewExpansionServiceRunner builds an ExpansionServiceRunner struct for a given gradle target and
// Beam version and returns a pointer to it. Passing an empty string as servicePort will request an
// open port to be assigned to the service.
func NewExpansionServiceRunner(jarPath, servicePort string) (*ExpansionServiceRunner, error) {
	if servicePort == "" {
		port, err := findOpenPort()
		if err != nil {
			return nil, fmt.Errorf("failed to find open port for service, got %v", err)
		}
		servicePort = fmt.Sprintf("%d", port)
	}
	serviceCommand := exec.Command("java", "-jar", jarPath, servicePort)
	return &ExpansionServiceRunner{execPath: jarPath, servicePort: servicePort, serviceCommand: serviceCommand}, nil
}

// NewPyExpansionServiceRunner builds an ExpansionServiceRunner struct for a given python module and
// Beam version and returns a pointer to it. Passing an empty string as servicePort will request an
// open port to be assigned to the service.
func NewPyExpansionServiceRunner(pythonExec, module, servicePort string) (*ExpansionServiceRunner, error) {
	if servicePort == "" {
		port, err := findOpenPort()
		if err != nil {
			return nil, fmt.Errorf("failed to find open port for service, got %v", err)
		}
		servicePort = fmt.Sprintf("%d", port)
	}
	serviceCommand := exec.Command(pythonExec, "-m", module, "-p", servicePort, "--fully_qualified_name_glob=*")
	return &ExpansionServiceRunner{execPath: pythonExec, servicePort: servicePort, serviceCommand: serviceCommand}, nil
}

func (e *ExpansionServiceRunner) String() string {
	return fmt.Sprintf("Exec Path: %v, Port: %v, Process: %v", e.execPath, e.servicePort, e.serviceCommand.Process)
}

// Endpoint returns the formatted endpoint the ExpansionServiceRunner is set to start the expansion
// service on.
func (e *ExpansionServiceRunner) Endpoint() string {
	return "localhost:" + e.servicePort
}

func (e *ExpansionServiceRunner) pingEndpoint(timeout time.Duration) error {
	ctx, canFunc := context.WithTimeout(context.Background(), timeout)
	defer canFunc()
	conn, err := grpc.DialContext(ctx, e.Endpoint(), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

const connectionTimeout = 15 * time.Second

// StartService starts the expansion service for a given ExpansionServiceRunner. If this is
// called and does not return an error, the expansion service will be running in the background
// until StopService is called. This will leak resources if not addressed.
func (e *ExpansionServiceRunner) StartService() error {
	err := e.serviceCommand.Start()
	if err != nil {
		return err
	}

	err = e.pingEndpoint(connectionTimeout)
	if err != nil {
		return err
	}
	return nil
}

// StopService stops the expansion service for a given ExpansionServiceRunner. Returns an error
// if the command hasn't been run or if the process has already exited.
func (e *ExpansionServiceRunner) StopService() error {
	expansionProcess := e.serviceCommand.Process
	if expansionProcess == nil {
		return fmt.Errorf("process does not exist for runner %v", e)
	}
	if e.serviceCommand.ProcessState != nil {
		return fmt.Errorf("process has already completed, state: %v", e)
	}
	return expansionProcess.Kill()
}
