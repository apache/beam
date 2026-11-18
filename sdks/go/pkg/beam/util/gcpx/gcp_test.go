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

package gcpx

import (
	"context"
	"errors"
	"testing"

	"golang.org/x/oauth2/google"
)

func TestGetUniverseDomain(t *testing.T) {
	tests := []struct {
		name      string
		env       string
		credsUD   string
		credsErr  error
		udErr     error
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
		{
			name:      "universe domain provider errors are not cached",
			udErr:     errors.New("metadata server error"),
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
					UniverseDomainProvider: func() (string, error) { return test.credsUD, test.udErr },
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
