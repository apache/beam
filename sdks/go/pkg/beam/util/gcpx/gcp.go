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

// Package gcpx contains utilities for working with Google Cloud Platform (GCP).
package gcpx

import (
	"context"
	"os"
	"sync"

	"golang.org/x/oauth2/google"
)

const universeDomainEnvVar = "GOOGLE_CLOUD_UNIVERSE_DOMAIN"

// findDefaultCredentials is a variable to allow overriding in tests.
var findDefaultCredentials = google.FindDefaultCredentials

// cachedUniverseDomain caches the universe domain resolved from the default
// credentials, since resolving it may require a metadata server round trip
// and clients are created frequently.
var cachedUniverseDomain struct {
	mu       sync.Mutex
	resolved bool
	value    string
}

// GetUniverseDomain returns the Google Cloud universe domain, first from the
// GOOGLE_CLOUD_UNIVERSE_DOMAIN environment variable, then falling back to the
// universe domain of the default credentials. Returns an empty string if it
// cannot be determined.
//
// Google API clients don't infer the universe domain from the credentials,
// and requests fail if the two differ, so clients should pass this value via
// option.WithUniverseDomain to support non-default universes such as Trusted
// Partner Cloud (TPC).
func GetUniverseDomain(ctx context.Context) string {
	if ud := os.Getenv(universeDomainEnvVar); ud != "" {
		return ud
	}

	cachedUniverseDomain.mu.Lock()
	defer cachedUniverseDomain.mu.Unlock()
	if cachedUniverseDomain.resolved {
		return cachedUniverseDomain.value
	}
	creds, err := findDefaultCredentials(ctx)
	if err != nil {
		// Don't cache: credentials may become available later, and client
		// construction surfaces the credential error itself.
		return ""
	}
	ud, err := creds.GetUniverseDomain()
	if err != nil {
		return ""
	}
	cachedUniverseDomain.resolved = true
	cachedUniverseDomain.value = ud
	return ud
}
