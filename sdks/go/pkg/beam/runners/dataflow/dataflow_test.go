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

package dataflow

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/jobopts"
	"sort"
	"testing"
)

func TestDontUseFlagAsPipelineOption(t *testing.T) {
	f := "dummy_flag"
	if flagFilter[f] {
		t.Fatalf("%q is already a flag, but should be unset", f)
	}
	DontUseFlagAsPipelineOption(f)
	if !flagFilter[f] {
		t.Fatalf("%q should be in the filter, but isn't set", f)
	}
}

func TestGetJobOptions(t *testing.T) {
	*labels = `{"label1": "val1", "label2": "val2"}`
	*stagingLocation = "gs://testStagingLocation"
	*autoscalingAlgorithm = "NONE"
	*minCPUPlatform = "testPlatform"

	*gcpopts.Project = "testProject"
	*gcpopts.Region = "testRegion"

	*jobopts.Experiments = "use_runner_v2,use_portable_job_submission"
	*jobopts.JobName = "testJob"

	opts, err := getJobOptions(nil)
	if err != nil {
		t.Fatalf("getJobOptions() returned error %q, want %q", err, "nil")
	}
	if got, want := opts.Name, "testJob"; got != want {
		t.Errorf("getJobOptions().Name = %q, want %q", got, want)
	}
	if got, want := len(opts.Experiments), 3; got != want {
		t.Errorf("len(getJobOptions().Experiments) = %q, want %q", got, want)
	} else {
		sort.Strings(opts.Experiments)
		expectedExperiments := []string{"min_cpu_platform=testPlatform", "use_portable_job_submission", "use_runner_v2"}
		for i := 0; i < 3; i++ {
			if got, want := opts.Experiments[i], expectedExperiments[i]; got != want {
				t.Errorf("getJobOptions().Experiments = %q, want %q", got, want)
			}
		}
	}
	if got, want := opts.Project, "testProject"; got != want {
		t.Errorf("getJobOptions().Project = %q, want %q", got, want)
	}
	if got, want := opts.Region, "testRegion"; got != want {
		t.Errorf("getJobOptions().Region = %q, want %q", got, want)
	}
	if got, want := len(opts.Labels), 2; got != want {
		t.Errorf("len(getJobOptions().Labels) = %q, want %q", got, want)
	} else {
		if got, want := opts.Labels["label1"], "val1"; got != want {
			t.Errorf("getJobOptions().Labels[\"label1\"] = %q, want %q", got, want)
		}
		if got, want := opts.Labels["label2"], "val2"; got != want {
			t.Errorf("getJobOptions().Labels[\"label2\"] = %q, want %q", got, want)
		}
	}
	if got, want := opts.TempLocation, "gs://testStagingLocation/tmp"; got != want {
		t.Errorf("getJobOptions().TempLocation = %q, want %q", got, want)
	}
}

func TestGetJobOptions_NoExperimentsSet(t *testing.T) {
	*labels = `{"label1": "val1", "label2": "val2"}`
	*stagingLocation = "gs://testStagingLocation"
	*autoscalingAlgorithm = "NONE"
	*minCPUPlatform = ""

	*gcpopts.Project = "testProject"
	*gcpopts.Region = "testRegion"

	*jobopts.Experiments = ""
	*jobopts.JobName = "testJob"

	opts, err := getJobOptions(nil)

	if err != nil {
		t.Fatalf("getJobOptions() returned error %q, want %q", err, "nil")
	}
	if got, want := len(opts.Experiments), 2; got != want {
		t.Fatalf("len(getJobOptions().Experiments) = %q, want %q", got, want)
	}
	sort.Strings(opts.Experiments)
	expectedExperiments := []string{"use_portable_job_submission", "use_unified_worker"}
	for i := 0; i < 2; i++ {
		if got, want := opts.Experiments[i], expectedExperiments[i]; got != want {
			t.Errorf("getJobOptions().Experiments = %q, want %q", got, want)
		}
	}
}

func TestGetJobOptions_NoStagingLocation(t *testing.T) {
	*stagingLocation = ""
	*gcpopts.Project = "testProject"
	*gcpopts.Region = "testRegion"

	_, err := getJobOptions(nil)
	if err == nil {
		t.Fatalf("getJobOptions() returned error nil, want an error")
	}
}

func TestGetJobOptions_InvalidAutoscaling(t *testing.T) {
	*labels = `{"label1": "val1", "label2": "val2"}`
	*stagingLocation = "gs://testStagingLocation"
	*autoscalingAlgorithm = "INVALID"
	*minCPUPlatform = "testPlatform"

	*gcpopts.Project = "testProject"
	*gcpopts.Region = "testRegion"

	*jobopts.Experiments = "use_runner_v2,use_portable_job_submission"
	*jobopts.JobName = "testJob"

	_, err := getJobOptions(nil)
	if err == nil {
		t.Fatalf("getJobOptions() returned error nil, want an error")
	}
}

func TestGetJobOptions_DockerNoImage(t *testing.T) {
	*jobopts.EnvironmentType = "docker"
	*jobopts.EnvironmentConfig = "testContainerImage"

	if got, want := getContainerImage(nil), "testContainerImage"; got != want {
		t.Fatalf("getContainerImage() = %q, want %q", got, want)
	}
}

func TestGetJobOptions_DockerWithImage(t *testing.T) {
	*jobopts.EnvironmentType = "docker"
	*jobopts.EnvironmentConfig = "testContainerImage"
	*image = "testContainerImageOverride"

	if got, want := getContainerImage(nil), "testContainerImageOverride"; got != want {
		t.Fatalf("getContainerImage() = %q, want %q", got, want)
	}
}
