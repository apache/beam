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

package gcsx

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
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

func setGcsEndpointFlag(t *testing.T, endpoint string) {
	t.Helper()
	orig := *gcpopts.GcsEndpoint
	*gcpopts.GcsEndpoint = endpoint
	t.Cleanup(func() { *gcpopts.GcsEndpoint = orig })
}

func TestUniverseDomain_EnvVarTakesPrecedence(t *testing.T) {
	resetUniverseDomainCache(t)
	t.Setenv(universeDomainEnvVar, "env.example.com")
	calls := fakeCredentials(t, "creds.example.com", nil)

	if got, want := universeDomain(context.Background(), storage.ScopeReadWrite), "env.example.com"; got != want {
		t.Errorf("universeDomain() = %q, want %q", got, want)
	}
	if *calls != 0 {
		t.Errorf("credentials looked up %d times, want 0", *calls)
	}
}

func TestUniverseDomain_FallsBackToCredentials(t *testing.T) {
	resetUniverseDomainCache(t)
	t.Setenv(universeDomainEnvVar, "")
	calls := fakeCredentials(t, "creds.example.com", nil)

	for i := 0; i < 2; i++ {
		if got, want := universeDomain(context.Background(), storage.ScopeReadWrite), "creds.example.com"; got != want {
			t.Errorf("universeDomain() = %q, want %q", got, want)
		}
	}
	if *calls != 1 {
		t.Errorf("credentials looked up %d times, want 1 (cached)", *calls)
	}
}

func TestUniverseDomain_CredentialsError(t *testing.T) {
	resetUniverseDomainCache(t)
	t.Setenv(universeDomainEnvVar, "")
	calls := fakeCredentials(t, "", errors.New("no credentials"))

	for i := 0; i < 2; i++ {
		if got := universeDomain(context.Background(), storage.ScopeReadWrite); got != "" {
			t.Errorf("universeDomain() = %q, want empty", got)
		}
	}
	if *calls != 2 {
		t.Errorf("credentials looked up %d times, want 2 (errors not cached)", *calls)
	}
}

func TestEndpointOptions(t *testing.T) {
	setGcsEndpointFlag(t, "")
	if got := endpointOptions(); len(got) != 0 {
		t.Errorf("endpointOptions() with no endpoint = %v, want none", got)
	}
	setGcsEndpointFlag(t, "https://storage.example.com/storage/v1/")
	if got := endpointOptions(); len(got) != 1 {
		t.Errorf("endpointOptions() with endpoint returned %d options, want 1", len(got))
	}
}

func TestNewUnauthenticatedClient_UsesGcsEndpoint(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"name": "some-bucket"}`))
	}))
	defer srv.Close()
	setGcsEndpointFlag(t, srv.URL+"/storage/v1/")

	ctx := context.Background()
	client, err := NewUnauthenticatedClient(ctx)
	if err != nil {
		t.Fatalf("NewUnauthenticatedClient() failed: %v", err)
	}
	defer client.Close()

	if _, err := client.Bucket("some-bucket").Attrs(ctx); err != nil {
		t.Fatalf("Attrs() failed: %v", err)
	}
	if !strings.HasPrefix(gotPath, "/storage/v1/b/some-bucket") {
		t.Errorf("request path = %q, want prefix %q", gotPath, "/storage/v1/b/some-bucket")
	}
}
