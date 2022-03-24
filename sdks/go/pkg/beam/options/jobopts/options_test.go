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

package jobopts

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core"
)

func TestGetEndpoint(t *testing.T) {
	address := "localhost:8099"
	Endpoint = &address
	v, err := GetEndpoint()
	if err != nil {
		t.Fatalf("unexpected error GetEndpoint(): %v", err)
	}
	if v != address {
		t.Errorf("GetEndpoint() = %v, want %v", v, address)
	}
}

func TestGetEndpoint_Bad(t *testing.T) {
	address := ""
	Endpoint = &address
	expectedErr := "no job service endpoint specified. Use --endpoint=<endpoint>"
	_, err := GetEndpoint()
	if v, ok := err.(missingFlagError); !ok {
		t.Errorf("GetEndpoint() executed incorrectly, got: %v, expected error: %s", v, expectedErr)
	}
}

func TestGetJobName(t *testing.T) {
	tests := []struct {
		jobname  string
		wantName string
	}{
		{
			"",
			"go-job-",
		},
		{
			"go-job-example",
			"go-job-example",
		},
	}
	for _, test := range tests {
		JobName = &test.jobname
		if got := GetJobName(); !strings.Contains(got, test.wantName) {
			t.Errorf("GetJobName() = %v, want %v", got, test.wantName)
		}
	}
}

func TestGetEnvironamentUrn(t *testing.T) {
	tests := []struct {
		env string
		urn string
	}{
		{
			"PROCESS",
			"beam:env:process:v1",
		},
		{
			"DOCKER",
			"beam:env:docker:v1",
		},
		{
			"LOOPBACK",
			"beam:env:external:v1",
		},
		{
			"",
			"beam:env:docker:v1",
		},
	}
	for _, test := range tests {
		EnvironmentType = &test.env
		if gotUrn := GetEnvironmentUrn(context.Background()); gotUrn != test.urn {
			t.Errorf("GetEnvironmentUrn(ctx) = %v, want %v", gotUrn, test.urn)
		}
	}
}

func TestGetEnvironmentConfig(t *testing.T) {
	tests := []struct {
		config string
		want   string
	}{
		{
			"apache/beam_go_sdk:2.34.0",
			"apache/beam_go_sdk:2.34.0",
		},
		{
			"",
			"apache/beam_go_sdk:" + core.SdkVersion,
		},
	}
	for _, test := range tests {
		EnvironmentConfig = &test.config
		if got := GetEnvironmentConfig(context.Background()); got != test.want {
			t.Errorf("GetEnvironmentConfig(ctx) = %v, want %v", got, test.want)
		}
	}
}

func TestGetSdkImageOverrides(t *testing.T) {
	var sdkOverrides stringSlice
	sdkOverrides.Set(".*beam_go_sdk.*, apache/beam_go_sdk:testing")
	SdkHarnessContainerImageOverrides = sdkOverrides
	want := map[string]string{".*beam_go_sdk.*": "apache/beam_go_sdk:testing"}
	if got := GetSdkImageOverrides(); reflect.DeepEqual(got, want) {
		t.Errorf("GetSdkImageOverrides() = %v, want %v", got, want)
	}
}
