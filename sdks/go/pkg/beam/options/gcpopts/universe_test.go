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
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
)

func TestGetUniverseDomain(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_UNIVERSE_DOMAIN", "env.example.com")
	if got, want := GetUniverseDomain(t.Context()), "env.example.com"; got != want {
		t.Errorf("GetUniverseDomain() = %q, want %q", got, want)
	}
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
