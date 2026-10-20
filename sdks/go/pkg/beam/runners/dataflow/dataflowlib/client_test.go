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

package dataflowlib

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// setupFakeCredentials points application default credentials at a fake
// authorized user credential file, so clients can be constructed without
// real credentials. No token is fetched until a request is made.
func setupFakeCredentials(t *testing.T) {
	t.Helper()
	credFile := filepath.Join(t.TempDir(), "creds.json")
	content := []byte(`{
		"type": "authorized_user",
		"client_id": "fake-client-id",
		"client_secret": "fake-client-secret",
		"refresh_token": "fake-refresh-token"
	}`)
	if err := os.WriteFile(credFile, content, 0600); err != nil {
		t.Fatalf("Failed writing fake credential file: %v", err)
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credFile)
}

func TestNewClient_BasePath(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		universe string
		want     string
	}{
		{
			name: "default",
			want: "https://dataflow.googleapis.com/",
		},
		{
			name:     "endpoint override",
			endpoint: "https://dataflow.example.com/",
			want:     "https://dataflow.example.com/",
		},
		{
			name:     "universe domain",
			universe: "example.com",
			want:     "https://dataflow.example.com/",
		},
		{
			name:     "endpoint takes precedence over universe domain",
			endpoint: "http://localhost:8080/",
			universe: "example.com",
			want:     "http://localhost:8080/",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setupFakeCredentials(t)
			t.Setenv("GOOGLE_CLOUD_UNIVERSE_DOMAIN", test.universe)

			client, err := NewClient(context.Background(), test.endpoint)
			if err != nil {
				t.Fatalf("NewClient(%q) failed: %v", test.endpoint, err)
			}
			if got := client.BasePath; got != test.want {
				t.Errorf("NewClient(%q).BasePath = %q, want %q", test.endpoint, got, test.want)
			}
		})
	}
}
