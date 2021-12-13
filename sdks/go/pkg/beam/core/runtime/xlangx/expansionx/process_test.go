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

func TestNewExpansionServiceRunner(t *testing.T) {
	testPath := "path/to/jar"
	serviceRunner := NewExpansionServiceRunner(testPath)
	if serviceRunner.jarPath != testPath {
		t.Errorf("JAR path mismatch: wanted %v, got %v", testPath, serviceRunner.jarPath)
	}
	commandString := strings.Join(serviceRunner.serviceCommand.Args, " ")
	expCommandString := "java -jar " + testPath + " 8097"
	if commandString != expCommandString {
		t.Errorf("command mismatch: wanted %v, got %v", expCommandString, commandString)
	}
}

func TestStartService_badCommand(t *testing.T) {
	serviceRunner := NewExpansionServiceRunner("")
	serviceRunner.serviceCommand = exec.Command("jahva", "-jar")
	err := serviceRunner.StartService()
	if err == nil {
		t.Error("StartService succeeded when it should have failed")
	}
}

func TestStartService_good(t *testing.T) {
	testPath := "path/to/jar"
	serviceRunner := NewExpansionServiceRunner(testPath)
	serviceRunner.serviceCommand = exec.Command("which", "go")
	err := serviceRunner.StartService()
	if err != nil {
		t.Errorf("StartService failed when it should have succeeded, got %v", err)
	}
}

func TestStopService_NoStart(t *testing.T) {
	processStruct := NewExpansionServiceRunner("test/path")
	err := processStruct.StopService()
	if err == nil {
		t.Errorf("StopService succeeded when it should have failed")
	}
}

func TestStopService_AlreadyStopped(t *testing.T) {
	processStruct := NewExpansionServiceRunner("test/path")
	// Set ProcessState to non-nil, indicating that the process exited
	processStruct.serviceCommand.ProcessState = &os.ProcessState{}
	err := processStruct.StopService()
	if err == nil {
		t.Errorf("StopSercvice succeeded when it should have failed")
	}
}
