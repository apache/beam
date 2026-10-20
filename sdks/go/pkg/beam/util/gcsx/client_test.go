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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
)

func setGcsEndpointFlag(t *testing.T, endpoint string) {
	t.Helper()
	orig := *gcpopts.GcsEndpoint
	*gcpopts.GcsEndpoint = endpoint
	t.Cleanup(func() { *gcpopts.GcsEndpoint = orig })
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
