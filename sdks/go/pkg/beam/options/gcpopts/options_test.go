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
	"context"
	"fmt"
	"os"
	"testing"
)

func TestGetProjectFromFlagOrEnvironment_Unset(t *testing.T) {
	setupFakeCredentialFile(t, "")
	*Project = ""
	if got, want := GetProjectFromFlagOrEnvironment(context.Background()), ""; got != want {
		t.Fatalf("GetProjectFromFlagOrEnvironment() = %q, want %q", got, want)
	}
}

func TestGetProjectFromFlagOrEnvironment_FlagSet(t *testing.T) {
	setupFakeCredentialFile(t, "")
	*Project = "test"
	if got, want := GetProjectFromFlagOrEnvironment(context.Background()), "test"; got != want {
		t.Fatalf("GetProjectFromFlagOrEnvironment() = %q, want %q", got, want)
	}
}

func TestGetProjectFromFlagOrEnvironment_EnvironmentSet(t *testing.T) {
	setupFakeCredentialFile(t, "fallback")
	*Project = ""
	if got, want := GetProjectFromFlagOrEnvironment(context.Background()), "fallback"; got != want {
		t.Fatalf("GetProjectFromFlagOrEnvironment() = %q, want %q", got, want)
	}
}

func TestGetProjectFromFlagOrEnvironment_BothSet(t *testing.T) {
	setupFakeCredentialFile(t, "fallback")
	*Project = "test"
	if got, want := GetProjectFromFlagOrEnvironment(context.Background()), "test"; got != want {
		t.Fatalf("GetProjectFromFlagOrEnvironment() = %q, want %q", got, want)
	}
}

func TestGetProject_FlagSet(t *testing.T) {
	setupFakeCredentialFile(t, "")
	*Project = "test"
	if got, want := GetProject(context.Background()), "test"; got != want {
		t.Fatalf("GetProject() = %q, want %q", got, want)
	}
}

func TestGetRegion_FlagSet(t *testing.T) {
	*Region = "test"
	if got, want := GetRegion(context.Background()), "test"; got != want {
		t.Fatalf("GetRegion() = %q, want %q", got, want)
	}
}

func TestGetRegion_EnvSet(t *testing.T) {
	*Region = ""
	os.Setenv("CLOUDSDK_COMPUTE_REGION", "envRegion")
	if got, want := GetRegion(context.Background()), "envRegion"; got != want {
		t.Fatalf("GetRegion() = %q, want %q", got, want)
	}
}

func TestGetRegion_BothSet(t *testing.T) {
	*Region = "test"
	os.Setenv("CLOUDSDK_COMPUTE_REGION", "envRegion")
	if got, want := GetRegion(context.Background()), "test"; got != want {
		t.Fatalf("GetRegion() = %q, want %q", got, want)
	}
}

// Set up fake credential file to read project from with the passed in projectId.
func setupFakeCredentialFile(t *testing.T, projectID string) {
	t.Helper()
	tmpFile, err := os.CreateTemp(os.TempDir(), "FAKE-GOOGLE-APPLICATION-CREDENTIALS-*.json")
	if err != nil {
		t.Fatalf("Failed creating fake credential file")
	}
	t.Cleanup(func() { os.Remove(tmpFile.Name()) })

	content := []byte(fmt.Sprintf(`{
		"type": "service_account",
		"project_id": %q
	  }`, projectID))
	if _, err := tmpFile.Write(content); err != nil {
		t.Fatalf("Failed writing to fake credential file")
	}
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", tmpFile.Name())
}
