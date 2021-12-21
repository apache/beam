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
	"fmt"
	"os/exec"
)

// ExpansionServiceRunner is a type that holds information required to
// start up a Beam Expansion Service JAR and maintain a handle on the
// process running the service to enable shutdown as well.
type ExpansionServiceRunner struct {
	jarPath        string
	servicePort    string
	serviceCommand *exec.Cmd
}

// NewExpansionServiceRunner builds an ExpansionServiceRunner struct for a given gradle target and
// Beam version and returns a pointer to it.
func NewExpansionServiceRunner(jarPath, servicePort string) *ExpansionServiceRunner {
	if servicePort == "" {
		servicePort = "8097"
	}
	serviceCommand := exec.Command("java", "-jar", jarPath, servicePort)
	return &ExpansionServiceRunner{jarPath: jarPath, servicePort: servicePort, serviceCommand: serviceCommand}
}

func (e *ExpansionServiceRunner) String() string {
	return fmt.Sprintf("JAR: %v, Port: %v, Process: %v", e.jarPath, e.servicePort, e.serviceCommand.Process)
}

// StartService starts the expansion service for a given ExpansionServiceRunner. If this is
// called and does not return an error, the expansion service will be running in the background
// until StopService is called. This will leak resources if not addressed.
func (e *ExpansionServiceRunner) StartService() error {
	err := e.serviceCommand.Start()
	if err != nil {
		return err
	}
	if e.serviceCommand.ProcessState != nil {
		return fmt.Errorf("process %v exited when it should still be running", e.serviceCommand.Process)
	}
	return nil
}

// StopService stops the expansion service for a given ExpansionServiceRunner. Returns an error
// if the command hasn't been run or if the process has already exited.
func (e *ExpansionServiceRunner) StopService() error {
	expansionProcess := e.serviceCommand.Process
	if expansionProcess == nil {
		return fmt.Errorf("Process does not exist for runner %v", e)
	}
	if e.serviceCommand.ProcessState != nil {
		return fmt.Errorf("Process has already completed, state: %v", e)
	}
	return expansionProcess.Kill()
}
