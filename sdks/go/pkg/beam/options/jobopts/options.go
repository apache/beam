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

// Package jobopts contains shared options for job submission. These options
// are exposed to allow user code to inspect and modify them.
package jobopts

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"sync/atomic"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/resource"
)

func init() {
	flag.Var(&SdkHarnessContainerImageOverrides,
		"sdk_harness_container_image_override",
		"Overrides for SDK harness container images. Could be for the "+
			"local SDK or for a remote SDK that pipeline has to support due "+
			"to a cross-language transform. Each entry consists of two values "+
			"separated by a comma where first value gives a regex to "+
			"identify the container image to override and the second value "+
			"gives the replacement container image. Multiple entries can be "+
			"specified by using this flag multiple times. A container will "+
			"have no more than 1 override applied to it. If multiple "+
			"overrides match a container image it is arbitrary which "+
			"will be applied.")
	flag.Var(&ResourceHints,
		"resource_hints",
		"Set whole pipeline level resource hints, accepting values of the format '<urn>=<value>'. "+
			"eg.'min_ram=12GB', 'beam:resources:accelerator:v1='runner_specific' "+
			"In case of duplicate hint URNs, the last value specified will be used. "+
			"See https://beam.apache.org/documentation/runtime/resource-hints/ for more information.")
}

var (
	// Endpoint is the job service endpoint.
	Endpoint = flag.String("endpoint", "", "Job service endpoint (required).")

	// JobName is the name of the job.
	JobName = flag.String("job_name", "", "Job name (optional).")

	// EnvironmentType is the environment type to run the user code.
	EnvironmentType = flag.String("environment_type", "DOCKER",
		"Environment Type. Possible options are DOCKER, and LOOPBACK.")

	// EnvironmentConfig is the environment configuration for running the user code.
	EnvironmentConfig = flag.String("environment_config",
		"",
		"Set environment configuration for running the user code.\n"+
			"For DOCKER: Url for the docker image.\n"+
			"For PROCESS: json of the form {\"os\": \"<OS>\", "+
			"\"arch\": \"<ARCHITECTURE>\", \"command\": \"<process to execute>\", "+
			"\"env\":{\"<Environment variables 1>\": \"<ENV_VAL>\"} }. "+
			"All fields in the json are optional except command.")

	// SdkHarnessContainerImageOverrides contains patterns for overriding
	// container image names in a pipeline.
	SdkHarnessContainerImageOverrides stringSlice

	// WorkerBinary is the location of the compiled worker binary. If not
	// specified, the binary is produced via go build.
	WorkerBinary = flag.String("worker_binary", "", "Worker binary (optional)")

	// Experiments toggle experimental features in the runner.
	Experiments = flag.String("experiments", "", "Comma-separated list of experiments (optional).")

	// Async determines whether to wait for job completion.
	Async = flag.Bool("async", false, "Do not wait for job completion.")

	// Strict mode applies additional validation to user pipelines before
	// executing them and fails early if the pipelines don't pass.
	Strict = flag.Bool("beam_strict", false, "Apply additional validation to pipelines.")

	// Flag to retain docker containers created by the runner. If false, then
	// containers are deleted once the job ends, even if it failed.
	RetainDockerContainers = flag.Bool("retain_docker_containers", false, "Retain Docker containers created by the runner.")

	// Flag to set the degree of parallelism. If not set, the configured Flink default is used, or 1 if none can be found.
	Parallelism = flag.Int("parallelism", -1, "The degree of parallelism to be used when distributing operations onto Flink workers.")

	// ResourceHints flag takes whole pipeline hints for resources.
	ResourceHints stringSlice
)

type missingFlagError error

// GetEndpoint returns the endpoint, if non empty and exits otherwise. Runners
// such as Dataflow set a reasonable default. Convenience function.
func GetEndpoint() (string, error) {
	if *Endpoint == "" {
		return "", missingFlagError(errors.New("no job service endpoint specified. Use --endpoint=<endpoint>"))
	}
	return *Endpoint, nil
}

var unique int32

// GetJobName returns the specified job name or, if not present, a fresh
// autogenerated name. Convenience function.
func GetJobName() string {
	if *JobName == "" {
		id := atomic.AddInt32(&unique, 1)
		return fmt.Sprintf("go-job-%v-%v", id, time.Now().UnixNano())
	}
	return *JobName
}

// GetEnvironmentUrn returns the specified EnvironmentUrn used to run the SDK Harness,
// if not present, returns the docker environment urn "beam:env:docker:v1".
// Convenience function.
func GetEnvironmentUrn(ctx context.Context) string {
	switch env := strings.ToLower(*EnvironmentType); env {
	case "process":
		return "beam:env:process:v1"
	case "loopback", "external":
		return "beam:env:external:v1"
	case "docker":
		return "beam:env:docker:v1"
	default:
		log.Infof(ctx, "No environment type specified. Using default environment: '%v'", *EnvironmentType)
		return "beam:env:docker:v1"
	}
}

// IsLoopback returns whether the EnvironmentType is loopback.
func IsLoopback() bool {
	return strings.ToLower(*EnvironmentType) == "loopback"
}

// GetEnvironmentConfig returns the specified configuration for specified SDK Harness,
// if not present, the default development container for the current user.
// Convenience function.
func GetEnvironmentConfig(ctx context.Context) string {
	if *EnvironmentConfig == "" {
		*EnvironmentConfig = core.DefaultDockerImage
		log.Infof(ctx, "No environment config specified. Using default config: '%v'", *EnvironmentConfig)
	}
	return *EnvironmentConfig
}

// GetSdkImageOverrides gets the specified overrides as a map where each key is
// a regular expression pattern to match, and each value is the string to
// replace matching containers with.
func GetSdkImageOverrides() map[string]string {
	ret := make(map[string]string)
	for _, pattern := range SdkHarnessContainerImageOverrides {
		splits := strings.SplitN(pattern, ",", 2)
		ret[splits[0]] = splits[1]
	}
	return ret
}

// GetExperiments returns the experiments.
func GetExperiments() []string {
	if *Experiments == "" {
		return nil
	}
	return strings.Split(*Experiments, ",")
}

// GetPipelineResourceHints parses known standard hints and returns the flag set hints for the pipeline.
// In case of duplicate hint URNs, the last value specified will be used.
func GetPipelineResourceHints() resource.Hints {
	hints := make([]resource.Hint, 0, len(ResourceHints))
	for _, hint := range ResourceHints {
		name, val, ok := strings.Cut(hint, "=")
		if !ok {
			panic(fmt.Sprintf("unparsable resource hint: %q", hint))
		}
		var h resource.Hint
		switch name {
		case "min_ram", "beam:resources:min_ram_bytes:v1":
			h = resource.ParseMinRAM(val)
		case "accelerator", "beam:resources:accelerator:v1":
			h = resource.Accelerator(val)
		default:
			if strings.HasPrefix(name, "beam:resources:") {
				h = stringHint{urn: name, value: val}
			} else {
				panic(fmt.Sprintf("unknown resource hint: %v", hint))
			}
		}
		hints = append(hints, h)
	}
	return resource.NewHints(hints...)
}

// stringHint is a backup implementation of hint for new standard hints.
type stringHint struct {
	urn, value string
}

func (h stringHint) URN() string {
	return h.urn
}

func (h stringHint) Payload() []byte {
	// Go strings are utf8, and if the string is ascii,
	// byte conversion handles that directly.
	return []byte(h.value)
}

func (h stringHint) MergeWithOuter(outer resource.Hint) resource.Hint {
	return h
}
