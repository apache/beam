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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"golang.org/x/oauth2/google"
)

func TestNewUnauthenticatedClient_UsesGcsEndpoint(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"name": "some-bucket"}`))
	}))
	defer srv.Close()

	runtime.GlobalOptions.Set("gcs_endpoint", srv.URL+"/storage/v1/")
	t.Cleanup(func() { runtime.GlobalOptions.Set("gcs_endpoint", "") })

	ctx := t.Context()
	client, err := NewUnauthenticatedClient(ctx)
	if err != nil {
		t.Fatalf("NewUnauthenticatedClient() failed: %v", err)
	}
	defer client.Close()

	if _, err := client.Bucket("some-bucket").Attrs(ctx); err != nil {
		t.Fatalf("Attrs() failed: %v", err)
	}
	if want := "/storage/v1/b/some-bucket"; !strings.HasPrefix(gotPath, want) {
		t.Errorf("request path = %q, want prefix %q", gotPath, want)
	}
}

func TestGetUniverseDomain(t *testing.T) {
	tests := []struct {
		name      string
		env       string
		credsUD   string
		credsErr  error
		want      string
		wantCalls int // credential lookups across two calls
	}{
		{
			name:    "env var takes precedence",
			env:     "env.example.com",
			credsUD: "creds.example.com",
			want:    "env.example.com",
		},
		{
			name:      "falls back to credentials and caches",
			credsUD:   "creds.example.com",
			want:      "creds.example.com",
			wantCalls: 1,
		},
		{
			name:      "credential errors are not cached",
			credsErr:  errors.New("no credentials"),
			want:      "",
			wantCalls: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resetUniverseDomainCache(t)
			t.Setenv(universeDomainEnvVar, test.env)
			calls := 0
			orig := findDefaultCredentials
			findDefaultCredentials = func(context.Context, ...string) (*google.Credentials, error) {
				calls++
				if test.credsErr != nil {
					return nil, test.credsErr
				}
				return &google.Credentials{
					UniverseDomainProvider: func() (string, error) { return test.credsUD, nil },
				}, nil
			}
			t.Cleanup(func() { findDefaultCredentials = orig })

			// Call twice so wantCalls can verify caching: a successful lookup
			// is reused by the second call, while a failed one is retried.
			for i := 0; i < 2; i++ {
				if got := GetUniverseDomain(t.Context()); got != test.want {
					t.Errorf("GetUniverseDomain() = %q, want %q", got, test.want)
				}
			}
			if calls != test.wantCalls {
				t.Errorf("credentials looked up %d times, want %d", calls, test.wantCalls)
			}
		})
	}
}

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
