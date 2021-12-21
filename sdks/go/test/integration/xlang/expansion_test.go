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

package xlang

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/xlangx/expansionx"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

const (
	// TODO(BEAN-13505): Select the most recent Beam release instead of a hard-coded
	// string.
	beamVersion   = "2.34.0"
	gradleTarget  = ":sdks:java:io:expansion-service:runExpansionService"
	expansionPort = "8097"
)

func checkPort(t *testing.T, port string) {
	ping := exec.Command("nc", "-vz", "localhost", port)
	output, err := ping.CombinedOutput()
	if err != nil {
		t.Errorf("failed to run ping to localhost:%v", port)
	}
	outputStr := string(output)
	if strings.Contains(outputStr, "failed") {
		t.Errorf("failed to connect to localhost:%v, got err %v", port, outputStr)
	}
}

func TestAutomatedExpansionService(t *testing.T) {
	integration.CheckFilters(t)
	jarPath, err := expansionx.GetBeamJar(gradleTarget, beamVersion)
	if err != nil {
		t.Fatalf("failed to get JAR path, got %v", err)
	}
	t.Cleanup(func() { os.Remove(jarPath) })

	serviceRunner := expansionx.NewExpansionServiceRunner(jarPath, expansionPort)
	err = serviceRunner.StartService()
	if err != nil {
		t.Errorf("failed to start expansion service JAR, got %v", err)
	}

	checkPort(t, expansionPort)

	err = serviceRunner.StopService()
	if err != nil {
		t.Errorf("failed to stop expansion service JAR, got %v", err)
	}
}
