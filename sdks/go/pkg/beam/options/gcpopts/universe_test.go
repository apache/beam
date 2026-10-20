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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/oauth2/google"
)

// resetUniverseDomainCache clears the cached universe domain and restores it
// when the test finishes.
func resetUniverseDomainCache(t *testing.T) {
	t.Helper()
	reset := func() {
		cachedUniverseDomain.mu.Lock()
		cachedUniverseDomain.resolved = false
		cachedUniverseDomain.value = ""
		cachedUniverseDomain.mu.Unlock()
	}
	reset()
	t.Cleanup(reset)
}

// fakeCredentials overrides findDefaultCredentials for the duration of the
// test, and returns a pointer to the number of times it was called.
func fakeCredentials(t *testing.T, universe string, err error) *int {
	t.Helper()
	calls := 0
	orig := findDefaultCredentials
	findDefaultCredentials = func(ctx context.Context, scopes ...string) (*google.Credentials, error) {
		calls++
		if err != nil {
			return nil, err
		}
		return &google.Credentials{
			UniverseDomainProvider: func() (string, error) { return universe, nil },
		}, nil
	}
	t.Cleanup(func() { findDefaultCredentials = orig })
	return &calls
}

func TestGetUniverseDomain_EnvVarTakesPrecedence(t *testing.T) {
	resetUniverseDomainCache(t)
	t.Setenv(universeDomainEnvVar, "env.example.com")
	calls := fakeCredentials(t, "creds.example.com", nil)

	if got, want := GetUniverseDomain(context.Background()), "env.example.com"; got != want {
		t.Errorf("GetUniverseDomain() = %q, want %q", got, want)
	}
	if *calls != 0 {
		t.Errorf("credentials looked up %d times, want 0", *calls)
	}
}

func TestGetUniverseDomain_FallsBackToCredentials(t *testing.T) {
	resetUniverseDomainCache(t)
	t.Setenv(universeDomainEnvVar, "")
	calls := fakeCredentials(t, "creds.example.com", nil)

	for i := 0; i < 2; i++ {
		if got, want := GetUniverseDomain(context.Background()), "creds.example.com"; got != want {
			t.Errorf("GetUniverseDomain() = %q, want %q", got, want)
		}
	}
	if *calls != 1 {
		t.Errorf("credentials looked up %d times, want 1 (cached)", *calls)
	}
}

func TestGetUniverseDomain_CredentialsError(t *testing.T) {
	resetUniverseDomainCache(t)
	t.Setenv(universeDomainEnvVar, "")
	calls := fakeCredentials(t, "", errors.New("no credentials"))

	for i := 0; i < 2; i++ {
		if got := GetUniverseDomain(context.Background()); got != "" {
			t.Errorf("GetUniverseDomain() = %q, want empty", got)
		}
	}
	if *calls != 2 {
		t.Errorf("credentials looked up %d times, want 2 (errors not cached)", *calls)
	}
}

func TestGetUniverseDomain_FromCredentialFile(t *testing.T) {
	resetUniverseDomainCache(t)
	t.Setenv(universeDomainEnvVar, "")

	credFile := filepath.Join(t.TempDir(), "creds.json")
	content := []byte(`{
		"type": "service_account",
		"project_id": "test-project",
		"universe_domain": "file.example.com"
	}`)
	if err := os.WriteFile(credFile, content, 0600); err != nil {
		t.Fatalf("Failed writing fake credential file: %v", err)
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credFile)

	if got, want := GetUniverseDomain(context.Background()), "file.example.com"; got != want {
		t.Errorf("GetUniverseDomain() = %q, want %q", got, want)
	}
}

func TestGetGcsEndpoint(t *testing.T) {
	orig := *GcsEndpoint
	t.Cleanup(func() { *GcsEndpoint = orig })

	*GcsEndpoint = "https://storage.example.com/storage/v1/"
	if got, want := GetGcsEndpoint(), "https://storage.example.com/storage/v1/"; got != want {
		t.Errorf("GetGcsEndpoint() = %q, want %q", got, want)
	}
}
