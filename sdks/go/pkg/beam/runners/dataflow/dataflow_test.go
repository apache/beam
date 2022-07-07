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
	"context"
	"sort"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/jobopts"
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
	resetGlobals()
	*labels = `{"label1": "val1", "label2": "val2"}`
	*stagingLocation = "gs://testStagingLocation"
	*minCPUPlatform = "testPlatform"
	*flexRSGoal = "FLEXRS_SPEED_OPTIMIZED"
	*dataflowServiceOptions = "opt1,opt2"

	*gcpopts.Project = "testProject"
	*gcpopts.Region = "testRegion"

	*jobopts.Experiments = "use_runner_v2,use_portable_job_submission"
	*jobopts.JobName = "testJob"

	opts, err := getJobOptions(context.Background())
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
	if got, want := len(opts.DataflowServiceOptions), 2; got != want {
		t.Errorf("len(getJobOptions().DataflowServiceOptions) = %q, want %q", got, want)
	} else {
		sort.Strings(opts.DataflowServiceOptions)
		expectedOptions := []string{"opt1", "opt2"}
		for i := 0; i < 2; i++ {
			if got, want := opts.DataflowServiceOptions[i], expectedOptions[i]; got != want {
				t.Errorf("getJobOptions().DataflowServiceOptions = %q, want %q", got, want)
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
	if got, want := opts.FlexRSGoal, "FLEXRS_SPEED_OPTIMIZED"; got != want {
		t.Errorf("getJobOptions().FlexRSGoal = %q, want %q", got, want)
	}
}

func TestGetJobOptions_NoExperimentsSet(t *testing.T) {
	resetGlobals()
	*stagingLocation = "gs://testStagingLocation"
	*gcpopts.Project = "testProject"
	*gcpopts.Region = "testRegion"
	*jobopts.Experiments = ""

	opts, err := getJobOptions(context.Background())

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
	resetGlobals()
	*stagingLocation = ""
	*gcpopts.Project = "testProject"
	*gcpopts.Region = "testRegion"

	_, err := getJobOptions(context.Background())
	if err == nil {
		t.Fatalf("getJobOptions() returned error nil, want an error")
	}
}

func TestGetJobOptions_InvalidAutoscaling(t *testing.T) {
	resetGlobals()
	*stagingLocation = "gs://testStagingLocation"
	*autoscalingAlgorithm = "INVALID"
	*gcpopts.Project = "testProject"
	*gcpopts.Region = "testRegion"

	_, err := getJobOptions(context.Background())
	if err == nil {
		t.Fatalf("getJobOptions() returned error nil, want an error")
	}
}

func TestGetJobOptions_InvalidRsGoal(t *testing.T) {
	resetGlobals()
	*stagingLocation = "gs://testStagingLocation"
	*flexRSGoal = "INVALID"
	*gcpopts.Project = "testProject"
	*gcpopts.Region = "testRegion"

	_, err := getJobOptions(context.Background())
	if err == nil {
		t.Fatalf("getJobOptions() returned error nil, want an error")
	}
}

func TestGetJobOptions_DockerWithImage(t *testing.T) {
	resetGlobals()
	*jobopts.EnvironmentType = "docker"
	*jobopts.EnvironmentConfig = "testContainerImage"
	*image = "testContainerImageOverride"

	if got, want := getContainerImage(context.Background()), "testContainerImageOverride"; got != want {
		t.Fatalf("getContainerImage() = %q, want %q", got, want)
	}
}

func TestGetJobOptions_DockerWithOldImage(t *testing.T) {
	resetGlobals()
	*jobopts.EnvironmentType = "docker"
	*jobopts.EnvironmentConfig = "testContainerImage"
	*workerHarnessImage = "testContainerImageOverride"

	if got, want := getContainerImage(context.Background()), "testContainerImageOverride"; got != want {
		t.Fatalf("getContainerImage() = %q, want %q", got, want)
	}
}

func TestGetJobOptions_DockerNoImage(t *testing.T) {
	resetGlobals()
	*jobopts.EnvironmentType = "docker"
	*jobopts.EnvironmentConfig = "testContainerImage"

	if got, want := getContainerImage(context.Background()), "testContainerImage"; got != want {
		t.Fatalf("getContainerImage() = %q, want %q", got, want)
	}
}

func TestGetJobOptions_TransformMapping(t *testing.T) {
	resetGlobals()
	*stagingLocation = "gs://testStagingLocation"
	*gcpopts.Project = "testProject"
	*gcpopts.Region = "testRegion"
	*update = true
	*transformMapping = `{"transformOne": "transformTwo"}`

	opts, err := getJobOptions(context.Background())
	if err != nil {
		t.Errorf("getJobOptions() returned error, got %v", err)
	}
	if opts == nil {
		t.Fatal("getJobOptions() got nil, want struct")
	}
	if got, ok := opts.TransformNameMapping["transformOne"]; !ok || got != "transformTwo" {
		t.Errorf("mismatch in transform mapping got %v, want %v", got, "transformTwo")
	}

}

func TestGetJobOptions_TransformMappingNoUpdate(t *testing.T) {
	resetGlobals()
	*stagingLocation = "gs://testStagingLocation"
	*gcpopts.Project = "testProject"
	*gcpopts.Region = "testRegion"
	*transformMapping = `{"transformOne": "transformTwo"}`

	opts, err := getJobOptions(context.Background())
	if err == nil {
		t.Error("getJobOptions() returned error nil, want an error")
	}
	if opts != nil {
		t.Errorf("getJobOptions() returned JobOptions when it should not have, got %#v, want nil", opts)
	}
}

func TestGetJobOptions_InvalidMapping(t *testing.T) {
	resetGlobals()
	*stagingLocation = "gs://testStagingLocation"
	*gcpopts.Project = "testProject"
	*gcpopts.Region = "testRegion"
	*update = true
	*transformMapping = "not a JSON-encoded string"

	opts, err := getJobOptions(context.Background())
	if err == nil {
		t.Error("getJobOptions() returned error nil, want an error")
	}
	if opts != nil {
		t.Errorf("getJobOptions() returned JobOptions when it should not have, got %#v, want nil", opts)
	}
}

func resetGlobals() {
	*autoscalingAlgorithm = ""
	*dataflowServiceOptions = ""
	*flexRSGoal = ""
	*gcpopts.Project = ""
	*gcpopts.Region = ""
	*image = ""
	*jobopts.EnvironmentType = ""
	*jobopts.EnvironmentConfig = ""
	*jobopts.Experiments = ""
	*jobopts.JobName = ""
	*labels = ""
	*minCPUPlatform = ""
	*stagingLocation = ""
	*transformMapping = ""
	*update = false
	*workerHarnessImage = ""
}
