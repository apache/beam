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
	"flag"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
)

func TestGetGcsEndpoint(t *testing.T) {
	const flagValue = "https://flag.example.com/storage/v1/"
	const optionValue = "https://option.example.com/storage/v1/"

	// On workers, the value arrives as a pipeline option rather than a flag.
	runtime.GlobalOptions.Set("gcs_endpoint", optionValue)
	t.Cleanup(func() {
		runtime.GlobalOptions.Set("gcs_endpoint", "")
	})

	if flag.Lookup("gcs_endpoint") == nil {
		if got := GetGcsEndpoint(); got != optionValue {
			t.Errorf("GetGcsEndpoint() with flag unregistered = %q, want pipeline option %q", got, optionValue)
		}
		flag.String("gcs_endpoint", "", "")
	}
	origFlag := flag.Lookup("gcs_endpoint").Value.String()
	t.Cleanup(func() {
		_ = flag.Set("gcs_endpoint", origFlag)
	})

	_ = flag.Set("gcs_endpoint", "")
	if got := GetGcsEndpoint(); got != optionValue {
		t.Errorf("GetGcsEndpoint() with flag unset = %q, want pipeline option %q", got, optionValue)
	}
	_ = flag.Set("gcs_endpoint", flagValue)
	if got := GetGcsEndpoint(); got != flagValue {
		t.Errorf("GetGcsEndpoint() with flag set = %q, want flag %q", got, flagValue)
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
