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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/apache/beam/sdks/v2/go/container/tools"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/artifact"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/execx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/grpcx"
)

var (
	// Contract: https://s.apache.org/beam-fn-api-container-contract.

	id                = flag.String("id", "", "Local identifier (required).")
	loggingEndpoint   = flag.String("logging_endpoint", "", "Local logging endpoint for FnHarness (required).")
	artifactEndpoint  = flag.String("artifact_endpoint", "", "Local artifact endpoint for FnHarness (required).")
	provisionEndpoint = flag.String("provision_endpoint", "", "Local provision endpoint for FnHarness (required).")
	controlEndpoint   = flag.String("control_endpoint", "", "Local control endpoint for FnHarness (required).")
	semiPersistDir    = flag.String("semi_persist_dir", "/tmp", "Local semi-persistent directory (optional).")
)

const entrypoint = "apache-beam-worker"

func main() {
	flag.Parse()
	if *id == "" {
		log.Fatal("No id provided.")
	}
	if *provisionEndpoint == "" {
		log.Fatal("No provision endpoint provided.")
	}

	ctx := grpcx.WriteWorkerID(context.Background(), *id)

	info, err := tools.ProvisionInfo(ctx, *provisionEndpoint)
	if err != nil {
		log.Fatalf("Failed to obtain provisioning information: %v", err)
	}
	log.Printf("Provision info:\n%v", info)

	// TODO(BEAM-8201): Simplify once flags are no longer used.
	if info.GetLoggingEndpoint().GetUrl() != "" {
		*loggingEndpoint = info.GetLoggingEndpoint().GetUrl()
	}
	if info.GetArtifactEndpoint().GetUrl() != "" {
		*artifactEndpoint = info.GetArtifactEndpoint().GetUrl()
	}
	if info.GetControlEndpoint().GetUrl() != "" {
		*controlEndpoint = info.GetControlEndpoint().GetUrl()
	}

	if *loggingEndpoint == "" {
		log.Fatal("No logging endpoint provided.")
	}
	if *artifactEndpoint == "" {
		log.Fatal("No artifact endpoint provided.")
	}
	if *controlEndpoint == "" {
		log.Fatal("No control endpoint provided.")
	}
	logger := &tools.Logger{Endpoint: *loggingEndpoint}
	logger.Printf(ctx, "Initializing typescript harness: %v", strings.Join(os.Args, " "))

	// (1) Obtain the pipeline options

	options, err := tools.ProtoToJSON(info.GetPipelineOptions())
	if err != nil {
		logger.Fatalf(ctx, "Failed to convert pipeline options: %v", err)
	}

	// (2) Retrieve and install the staged packages.

	dir := filepath.Join(*semiPersistDir, *id, "staged")
	artifacts, err := artifact.Materialize(ctx, *artifactEndpoint, info.GetDependencies(), info.GetRetrievalToken(), dir)
	if err != nil {
		logger.Fatalf(ctx, "Failed to retrieve staged files: %v", err)
	}

	// Create a package.json that names given dependencies as overrides.
	npmOverrides := make(map[string]string)
	for _, v := range artifacts {
		name, _ := artifact.MustExtractFilePayload(v)
		path := filepath.Join(dir, name)
		if v.RoleUrn == "beam:artifact:type:npm_dep:v1" {
			// Npm cannot handle arbitrary suffixes.
			suffixedPath := path + ".tar"
			if err := os.Rename(path, suffixedPath); err != nil {
				logger.Fatalf(ctx, "unable to rename %v to %v: %v", path, suffixedPath, err)
			}
			npmOverrides[string(v.RolePayload)] = suffixedPath
		}
	}
	if len(npmOverrides) > 0 {
		f, err := os.Create("package.json")
		if err != nil {
			logger.Fatalf(ctx, "unable to os.Create(%q): %v", "package.json", err)
		}
		defer f.Close()
		f.WriteString("{\n")
		f.WriteString("  \"name\": \"beam-worker\",\n  \"version\": \"1.0.0\",\n")
		f.WriteString("  \"overrides\": {\n")
		needsComma := false
		for pkg, path := range npmOverrides {
			if needsComma {
				f.WriteString(",")
			} else {
				needsComma = true
			}
			f.WriteString(fmt.Sprintf("    %q: %q\n", pkg, "file:"+path))
		}
		f.WriteString("  }\n")
		f.WriteString("}\n")
	}
	execx.Execute("cat", "package.json")

	// Now install any other npm packages.
	for _, v := range artifacts {
		name, _ := artifact.MustExtractFilePayload(v)
		path := filepath.Join(dir, name)
		if v.RoleUrn == "beam:artifact:type:npm:v1" {
			// Npm cannot handle arbitrary suffixes.
			suffixedPath := path + ".tar"
			if err := os.Rename(path, suffixedPath); err != nil {
				logger.Fatalf(ctx, "unable to rename %v to %v: %v", path, suffixedPath, err)
			}
			if err := execx.Execute("npm", "install", suffixedPath); err != nil {
				logger.Fatalf(ctx, "Error installing package %q: %v", suffixedPath, err)
			}
		}
	}

	// (3) Invoke the Node entrypoint, passing the Fn API container contract info as flags.

	args := []string{
		entrypoint,
		"--logging_endpoint=" + *loggingEndpoint,
		"--control_endpoint=" + *controlEndpoint,
		"--semi_persist_dir=" + *semiPersistDir,
		"--options=" + options,
	}

	if info.GetStatusEndpoint() != nil {
		args = append(args, "--status_endpoint="+info.GetStatusEndpoint().GetUrl())
	}

	workerIds := append([]string{*id}, info.GetSiblingWorkerIds()...)
	var wg sync.WaitGroup
	wg.Add(len(workerIds))
	for _, workerId := range workerIds {
		go func(workerId string) {
			workerArgs := append(append([]string{}, args...), "--id="+workerId)
			logger.Printf(ctx, "Executing: npx %v", strings.Join(workerArgs, " "))
			logger.Fatalf(ctx, "User program exited: %v", execx.Execute("npx", workerArgs...))
		}(workerId)
	}
	wg.Wait()
}
