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

// Package gcpopts contains shared options for Google Cloud Platform.
package gcpopts

import (
	"context"
	"flag"
	"os"
	"os/exec"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

var (
	// Project is the Google Cloud Platform project ID.
	Project = flag.String("project", "", "Google Cloud Platform project ID.")

	// Region is the GCP region where the job should be run.
	Region = flag.String("region", "", "GCP Region (required)")
)

// GetProject returns the project, if non empty and exits otherwise.
// Convenience function.
func GetProject(ctx context.Context) string {
	if *Project == "" {
		log.Exit(ctx, "No Google Cloud project specified. Use --project=<project>")
	}
	return *Project
}

// GetRegion returns the region first via flag then falling back to
// https://cloud.google.com/compute/docs/gcloud-compute/#default-properties.
func GetRegion(ctx context.Context) string {
	if *Region == "" {
		env := os.Getenv("CLOUDSDK_COMPUTE_REGION")
		if env != "" {
			log.Infof(ctx, "Using default GCP region %s from $CLOUDSDK_COMPUTE_REGION.", env)
			return env
		}

		cmd := exec.CommandContext(ctx, "gcloud", "config", "get-value", "compute/region")
		if out, err := cmd.Output(); err == nil && len(out) > 0 {
			region := strings.TrimSpace(string(out))
			// TODO(bamnet): Replace hardcoded gcloud reference with `cmd.String()` when
			// BeamModulePlugin.groovy supports Go >= 1.13. String() did not exist in 1.12.
			log.Infof(ctx, "Using default GCP region %s from gcloud output.", region)
			return region
		}
	}

	return *Region
}
