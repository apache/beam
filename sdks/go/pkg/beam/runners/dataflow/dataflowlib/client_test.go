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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

func TestNewClient_BasePath(t *testing.T) {
	// Fake service account credentials in a non-default universe. The auth
	// library parses the key when the client is created (non-default universes
	// use self-signed JWTs), so it must be a valid key. No token is fetched
	// until a request is made.
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed generating key: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("Failed marshaling key: %v", err)
	}
	content, err := json.Marshal(map[string]string{
		"type":            "service_account",
		"project_id":      "test-project",
		"client_email":    "test@test-project.iam.gserviceaccount.com",
		"private_key_id":  "test-key-id",
		"private_key":     string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"universe_domain": "creds.example.com",
	})
	if err != nil {
		t.Fatalf("Failed marshaling credentials: %v", err)
	}
	credFile := filepath.Join(t.TempDir(), "creds.json")
	if err := os.WriteFile(credFile, content, 0600); err != nil {
		t.Fatalf("Failed writing fake credential file: %v", err)
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credFile)

	tests := []struct {
		name     string
		endpoint string
		env      string
		want     string
	}{
		{
			// The client library doesn't read the credentials' universe domain,
			// so this verifies that NewClient passes it through.
			name: "universe domain from credentials",
			want: "https://dataflow.creds.example.com/",
		},
		{
			name:     "endpoint takes precedence over universe domain",
			endpoint: "http://localhost:8080/",
			env:      "creds.example.com",
			want:     "http://localhost:8080/",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("GOOGLE_CLOUD_UNIVERSE_DOMAIN", test.env)

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
