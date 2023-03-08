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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/resource"
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

// Also tests IsLoopback because it uses the same flag.
func TestGetEnvironmentUrn(t *testing.T) {
	tests := []struct {
		env        string
		urn        string
		isLoopback bool
	}{
		{
			"PROCESS",
			"beam:env:process:v1",
			false,
		},
		{
			"DOCKER",
			"beam:env:docker:v1",
			false,
		},
		{
			"LOOPBACK",
			"beam:env:external:v1",
			true,
		},
		{
			"",
			"beam:env:docker:v1",
			false,
		},
	}
	for _, test := range tests {
		EnvironmentType = &test.env
		if got, want := GetEnvironmentUrn(context.Background()), test.urn; got != want {
			t.Errorf("GetEnvironmentUrn(%v) = %v, want %v", test.env, got, want)
		}
		if got, want := IsLoopback(), test.isLoopback; got != want {
			t.Errorf("IsLoopback(%v) = %v, want %v", test.env, got, want)
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
			core.DefaultDockerImage,
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

func TestGetPipelineResourceHints(t *testing.T) {
	var hints stringSlice
	hints.Set("min_ram=2GB")
	hints.Set("beam:resources:min_ram_bytes:v1=16GB")
	hints.Set("beam:resources:accelerator:v1=cheetah")
	hints.Set("accelerator=pedal_to_the_metal")
	hints.Set("beam:resources:novel_execution:v1=jaguar")
	hints.Set("min_ram=1GB")
	ResourceHints = hints

	want := resource.NewHints(resource.ParseMinRAM("1GB"), resource.Accelerator("pedal_to_the_metal"), stringHint{
		urn:   "beam:resources:novel_execution:v1",
		value: "jaguar",
	})
	if got := GetPipelineResourceHints(); !got.Equal(want) {
		t.Errorf("GetPipelineResourceHints() = %v, want %v", got, want)
	}
}

func TestGetExperiements(t *testing.T) {
	*Experiments = ""
	if got, want := GetExperiments(), []string(nil); !reflect.DeepEqual(got, want) {
		t.Errorf("GetExperiments(\"\") = %v, want %v", got, want)
	}
	*Experiments = "better,faster,stronger"
	if got, want := GetExperiments(), []string{"better", "faster", "stronger"}; !reflect.DeepEqual(got, want) {
		t.Errorf("GetExperiments(\"\") = %v, want %v", got, want)
	}
}
