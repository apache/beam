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
	"strconv"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/xlangx/expansionx"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

const (
	gradleTarget = ":sdks:java:io:expansion-service:runExpansionService"
)

func getTargetVersion(currentVersion string) string {
	curVersion := strings.Split(currentVersion, ".")
	minorVersion, _ := strconv.Atoi(curVersion[1])
	targetMinor := minorVersion - 2
	curVersion[1] = strconv.Itoa(targetMinor)
	return strings.Join(curVersion[0:3], ".")
}

func TestGetTargetVersion(t *testing.T) {
	var tests = []struct {
		name      string
		input     string
		expOutput string
	}{
		{
			"normal version",
			"2.50.0",
			"2.48.0",
		},
		{
			"dev version",
			"2.10.0.dev",
			"2.8.0",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, want := getTargetVersion(test.input), test.expOutput; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
		})
	}
}

func TestAutomatedExpansionService(t *testing.T) {
	integration.CheckFilters(t)
	beamVersion := getTargetVersion(core.SdkVersion)
	jarPath, err := expansionx.GetBeamJar(gradleTarget, beamVersion)
	if err != nil {
		t.Fatalf("failed to get JAR path, got %v", err)
	}
	t.Cleanup(func() { os.Remove(jarPath) })

	serviceRunner, err := expansionx.NewExpansionServiceRunner(jarPath, "")
	if err != nil {
		t.Fatalf("failed to make new expansion service runner, got %v", err)
	}
	err = serviceRunner.StartService()
	if err != nil {
		t.Errorf("failed to start expansion service JAR, got %v", err)
	}

	err = serviceRunner.StopService()
	if err != nil {
		t.Errorf("failed to stop expansion service JAR, got %v", err)
	}
}
