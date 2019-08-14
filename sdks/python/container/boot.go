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

// boot is the boot code for the Python SDK harness container. It is responsible
// for retrieving and install staged files and invoking python correctly.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/artifact"
	pbjob "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	pbpipeline "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/provision"
	"github.com/apache/beam/sdks/go/pkg/beam/util/execx"
	"github.com/apache/beam/sdks/go/pkg/beam/util/grpcx"
	"github.com/golang/protobuf/proto"
)

var (
	acceptableWhlSpecs = []string{"cp27-cp27mu-manylinux1_x86_64.whl"}

	// Contract: https://s.apache.org/beam-fn-api-container-contract.

	workerPool        = flag.Bool("worker_pool", false, "Run as worker pool (optional).")
	id                = flag.String("id", "", "Local identifier (required).")
	loggingEndpoint   = flag.String("logging_endpoint", "", "Logging endpoint (required).")
	artifactEndpoint  = flag.String("artifact_endpoint", "", "Artifact endpoint (required).")
	provisionEndpoint = flag.String("provision_endpoint", "", "Provision endpoint (required).")
	controlEndpoint   = flag.String("control_endpoint", "", "Control endpoint (required).")
	semiPersistDir    = flag.String("semi_persist_dir", "/tmp", "Local semi-persistent directory (optional).")
)

const (
	sdkHarnessEntrypoint = "apache_beam.runners.worker.sdk_worker_main"
	// Please keep these names in sync with stager.py
	workflowFile      = "workflow.tar.gz"
	requirementsFile  = "requirements.txt"
	sdkSrcFile        = "dataflow_python_sdk.tar"
	extraPackagesFile = "extra_packages.txt"
)

func main() {
	flag.Parse()

	if *workerPool == true {
		args := []string{
			"-m",
			"apache_beam.runners.worker.worker_pool_main",
			"--service_port=50000",
			"--container_executable=/opt/apache/beam/boot",
		}
		log.Printf("Starting Python SDK worker pool: python %v", strings.Join(args, " "))
		log.Fatalf("Python SDK worker pool exited: %v", execx.Execute("python", args...))
	}

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

	log.Printf("Initializing python harness: %v", strings.Join(os.Args, " "))

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

	// (2) Retrieve and install the staged packages.

	dir := filepath.Join(*semiPersistDir, "staged")

	files, err := artifact.Materialize(ctx, *artifactEndpoint, info.GetRetrievalToken(), dir)
	if err != nil {
		log.Fatalf("Failed to retrieve staged files: %v", err)
	}

	// TODO(herohde): the packages to install should be specified explicitly. It
	// would also be possible to install the SDK in the Dockerfile.
	if setupErr := installSetupPackages(files, dir); setupErr != nil {
		log.Fatalf("Failed to install required packages: %v", setupErr)
	}

	// (3) Invoke python

	os.Setenv("WORKER_ID", *id)
	os.Setenv("PIPELINE_OPTIONS", options)
	os.Setenv("SEMI_PERSISTENT_DIRECTORY", *semiPersistDir)
	os.Setenv("LOGGING_API_SERVICE_DESCRIPTOR", proto.MarshalTextString(&pbpipeline.ApiServiceDescriptor{Url: *loggingEndpoint}))
	os.Setenv("CONTROL_API_SERVICE_DESCRIPTOR", proto.MarshalTextString(&pbpipeline.ApiServiceDescriptor{Url: *controlEndpoint}))

	args := []string{
		"-m",
		sdkHarnessEntrypoint,
	}
	log.Printf("Executing: python %v", strings.Join(args, " "))

	log.Fatalf("Python exited: %v", execx.Execute("python", args...))
}

// installSetupPackages installs Beam SDK and user dependencies.
func installSetupPackages(mds []*pbjob.ArtifactMetadata, workDir string) error {
	log.Printf("Installing setup packages ...")

	files := make([]string, len(mds))
	for i, v := range mds {
		log.Printf("Found artifact: %s", v.Name)
		files[i] = v.Name
	}

	// Install the Dataflow Python SDK and worker packages.
	// We install the extra requirements in case of using the beam sdk. These are ignored by pip
	// if the user is using an SDK that does not provide these.
	if err := installSdk(files, workDir, sdkSrcFile, acceptableWhlSpecs, false); err != nil {
		return fmt.Errorf("failed to install SDK: %v", err)
	}
	// The staged files will not disappear due to restarts because workDir is a
	// folder that is mapped to the host (and therefore survives restarts).
	if err := pipInstallRequirements(files, workDir, requirementsFile); err != nil {
		return fmt.Errorf("failed to install requirements: %v", err)
	}
	if err := installExtraPackages(files, extraPackagesFile, workDir); err != nil {
		return fmt.Errorf("failed to install extra packages: %v", err)
	}
	if err := pipInstallPackage(files, workDir, workflowFile, false, true, nil); err != nil {
		return fmt.Errorf("failed to install workflow: %v", err)
	}

	return nil
}

// joinPaths joins the dir to every artifact path. Each / in the path is
// interpreted as a directory separator.
func joinPaths(dir string, paths ...string) []string {
	var ret []string
	for _, p := range paths {
		ret = append(ret, filepath.Join(dir, filepath.FromSlash(p)))
	}
	return ret
}
