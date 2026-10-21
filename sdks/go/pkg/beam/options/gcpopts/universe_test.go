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
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"golang.org/x/oauth2/google"
)

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

func TestGetGcsEndpoint(t *testing.T) {
	const flagValue = "https://flag.example.com/storage/v1/"
	const optionValue = "https://option.example.com/storage/v1/"

	origFlag := *GcsEndpoint
	t.Cleanup(func() {
		*GcsEndpoint = origFlag
		runtime.GlobalOptions.Set("gcs_endpoint", "")
	})
	// On workers, the value arrives as a pipeline option rather than a flag.
	runtime.GlobalOptions.Set("gcs_endpoint", optionValue)

	*GcsEndpoint = ""
	if got := GetGcsEndpoint(); got != optionValue {
		t.Errorf("GetGcsEndpoint() with flag unset = %q, want pipeline option %q", got, optionValue)
	}
	*GcsEndpoint = flagValue
	if got := GetGcsEndpoint(); got != flagValue {
		t.Errorf("GetGcsEndpoint() with flag set = %q, want flag %q", got, flagValue)
	}
}
