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

package gcpopts

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestGetProjectFromFlagOrEnvironmentWithNoProjectSet(t *testing.T) {
	*Project = ""
	ret := GetProjectFromFlagOrEnvironment(nil)
	if ret != "" {
		t.Fatalf("%q returned as project id, should be \"\"", ret)
	}
}

func TestGetProjectFromFlagOrEnvironmentWithProjectFlagSet(t *testing.T) {
	*Project = "test"
	ret := GetProjectFromFlagOrEnvironment(nil)
	if ret != "test" {
		t.Fatalf("%q returned as project id, should be \"test\"", ret)
	}
}

func TestGetProjectFromFlagOrEnvironmentWithNoProjectFlagSetAndFallbackSet(t *testing.T) {
	setupFakeCredentialFile(t, "fallback")
	*Project = ""
	ret := GetProjectFromFlagOrEnvironment(nil)
	if ret != "fallback" {
		t.Fatalf("%q returned as project id, should be \"fallback\"", ret)
	}
}

func TestGetProjectFromFlagOrEnvironmentWithProjectFlagSetAndFallbackSet(t *testing.T) {
	setupFakeCredentialFile(t, "fallback")
	*Project = "test"
	ret := GetProjectFromFlagOrEnvironment(nil)
	if ret == "fallback" {
		t.Fatalf("fallback returned as project id, should have used the flag setting of test")
	}
	if ret != "test" {
		t.Fatalf("%q returned as project id, should be \"test\"", ret)
	}
}

// Set up fake credential file to read project from.
func setupFakeCredentialFile(t *testing.T, projectId string) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "FAKE-GOOGLE-APPLICATION-CREDENTIALS-*.json")
	if err != nil {
		t.Fatalf("Failed creating fake credential file")
	}
	t.Cleanup(func() { os.Remove(tmpFile.Name()) })

	content := []byte(`{
		"type": "service_account",
		"project_id": "fallback"
	  }`)
	if _, err := tmpFile.Write(content); err != nil {
		t.Fatalf("Failed writing to fake credential file")
	}
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", tmpFile.Name())
}
