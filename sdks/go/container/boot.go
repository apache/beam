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
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/artifact"
	"github.com/apache/beam/sdks/go/pkg/beam/provision"
	"github.com/apache/beam/sdks/go/pkg/beam/util/execx"
	"github.com/apache/beam/sdks/go/pkg/beam/util/grpcx"
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

func main() {
	flag.Parse()
	if *id == "" {
		log.Fatal("No id provided.")
	}
	if *loggingEndpoint == "" {
		log.Fatal("No logging endpoint provided.")
	}
	if *artifactEndpoint == "" {
		log.Fatal("No artifact endpoint provided.")
	}
	if *provisionEndpoint == "" {
		log.Fatal("No provision endpoint provided.")
	}
	if *controlEndpoint == "" {
		log.Fatal("No control endpoint provided.")
	}

	log.Printf("Initializing Go harness: %v", strings.Join(os.Args, " "))

	ctx := grpcx.WriteWorkerID(context.Background(), *id)

	// (1) Obtain the pipeline options

	info, err := provision.Info(ctx, *provisionEndpoint)
	if err != nil {
		log.Fatalf("Failed to obtain provisioning information: %v", err)
	}
	options, err := provision.ProtoToJSON(info.GetPipelineOptions())
	if err != nil {
		log.Fatalf("Failed to convert pipeline options: %v", err)
	}

	// (2) Retrieve the staged files.
	//
	// The Go SDK harness downloads the worker binary and invokes
	// it. For now, we assume that the first (and only) package
	// is the binary.

	dir := filepath.Join(*semiPersistDir, "staged")
	artifacts, err := artifact.Materialize(ctx, *artifactEndpoint, dir)
	if err != nil {
		log.Fatalf("Failed to retrieve staged files: %v", err)
	}
	if len(artifacts) == 0 {
		log.Fatal("No binaries staged")
	}

	// (3) The persist dir may be on a noexec volume, so we must
	// copy the binary to a different location to execute.

	prog := filepath.Join("/bin", artifacts[0].Name)
	if err := copyExe(filepath.Join(dir, artifacts[0].Name), prog); err != nil {
		log.Fatalf("Failed to copy binary: %v", err)
	}

	args := []string{
		"--worker=true",
		"--id=" + *id,
		"--logging_endpoint=" + *loggingEndpoint,
		"--control_endpoint=" + *controlEndpoint,
		"--semi_persist_dir=" + *semiPersistDir,
		"--options=" + options,
	}
	log.Fatalf("User program exited: %v", execx.Execute(prog, args...))
}

func copyExe(from, to string) error {
	src, err := os.Open(from)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return err
	}

	if _, err := io.Copy(dst, src); err != nil {
		return err
	}
	return dst.Close()
}
