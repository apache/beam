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
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/artifact"
	jobpb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/provision"
	"github.com/apache/beam/sdks/go/pkg/beam/util/execx"
	"github.com/apache/beam/sdks/go/pkg/beam/util/grpcx"
	"github.com/golang/protobuf/proto"
	"github.com/nightlyone/lockfile"
)

var (
	acceptableWhlSpecs []string

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
	workerPoolIdEnv   = "BEAM_PYTHON_WORKER_POOL_ID"
)

func main() {
	flag.Parse()

	if *workerPool == true {
		workerPoolId := fmt.Sprintf("%d", os.Getpid())
		os.Setenv(workerPoolIdEnv, workerPoolId)
		args := []string{
			"-m",
			"apache_beam.runners.worker.worker_pool_main",
			"--service_port=50000",
			"--container_executable=/opt/apache/beam/boot",
		}
		log.Printf("Starting worker pool %v: python %v", workerPoolId, strings.Join(args, " "))
		log.Fatalf("Python SDK worker pool exited: %v", execx.Execute("python", args...))
	}

	if *id == "" {
		log.Fatal("No id provided.")
	}
	if *provisionEndpoint == "" {
		log.Fatal("No provision endpoint provided.")
	}

	ctx := grpcx.WriteWorkerID(context.Background(), *id)

	info, err := provision.Info(ctx, *provisionEndpoint)
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

	log.Printf("Initializing python harness: %v", strings.Join(os.Args, " "))

	// (1) Obtain the pipeline options

	options, err := provision.ProtoToJSON(info.GetPipelineOptions())
	if err != nil {
		log.Fatalf("Failed to convert pipeline options: %v", err)
	}

	// (2) Retrieve and install the staged packages.
	//
	// Guard from concurrent artifact retrieval and installation,
	// when called by child processes in a worker pool.

	if err := setupAcceptableWheelSpecs(); err != nil {
		log.Printf("Failed to setup acceptable wheel specs, leave it as empty: %v", err)
	}

	materializeArtifactsFunc := func() {
		dir := filepath.Join(*semiPersistDir, "staged")

		files, err := artifact.Materialize(ctx, *artifactEndpoint, info.GetDependencies(), info.GetRetrievalToken(), dir)
		if err != nil {
			log.Fatalf("Failed to retrieve staged files: %v", err)
		}

		// TODO(herohde): the packages to install should be specified explicitly. It
		// would also be possible to install the SDK in the Dockerfile.
		if setupErr := installSetupPackages(files, dir); setupErr != nil {
			log.Fatalf("Failed to install required packages: %v", setupErr)
		}
	}

	workerPoolId := os.Getenv(workerPoolIdEnv)
	if workerPoolId != "" {
		multiProcessExactlyOnce(materializeArtifactsFunc, "beam.install.complete."+workerPoolId)
	} else {
		materializeArtifactsFunc()
	}

	// (3) Invoke python

	os.Setenv("WORKER_ID", *id)
	os.Setenv("PIPELINE_OPTIONS", options)
	os.Setenv("SEMI_PERSISTENT_DIRECTORY", *semiPersistDir)
	os.Setenv("LOGGING_API_SERVICE_DESCRIPTOR", proto.MarshalTextString(&pipepb.ApiServiceDescriptor{Url: *loggingEndpoint}))
	os.Setenv("CONTROL_API_SERVICE_DESCRIPTOR", proto.MarshalTextString(&pipepb.ApiServiceDescriptor{Url: *controlEndpoint}))

	if info.GetStatusEndpoint() != nil {
		os.Setenv("STATUS_API_SERVICE_DESCRIPTOR", proto.MarshalTextString(info.GetStatusEndpoint()))
	}

	args := []string{
		"-m",
		sdkHarnessEntrypoint,
	}
	log.Printf("Executing: python %v", strings.Join(args, " "))

	log.Fatalf("Python exited: %v", execx.Execute("python", args...))
}

// setup wheel specs according to installed python version
func setupAcceptableWheelSpecs() error {
	cmd := exec.Command("python", "-V")
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		return err
	}
	re := regexp.MustCompile(`Python (\d)\.(\d).*`)
	pyVersions := re.FindStringSubmatch(string(stdoutStderr[:]))
	if len(pyVersions) != 3 {
		return fmt.Errorf("cannot get parse Python version from %s", stdoutStderr)
	}
	pyVersion := fmt.Sprintf("%s%s", pyVersions[1], pyVersions[2])
	var wheelName string
	switch pyVersion {
	case "27":
		wheelName = "cp27-cp27mu-manylinux1_x86_64.whl"
	case "35", "36", "37":
		wheelName = fmt.Sprintf("cp%s-cp%sm-manylinux1_x86_64.whl", pyVersion, pyVersion)
	default:
		wheelName = fmt.Sprintf("cp%s-cp%s-manylinux1_x86_64.whl", pyVersion, pyVersion)
	}
	acceptableWhlSpecs = append(acceptableWhlSpecs, wheelName)
	return nil
}

// installSetupPackages installs Beam SDK and user dependencies.
func installSetupPackages(mds []*jobpb.ArtifactMetadata, workDir string) error {
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

// Call the given function exactly once across multiple worker processes.
// The need for multiple processes is specific to the Python SDK due to the GIL.
// Should another SDK require it, this could be separated out as shared utility.
func multiProcessExactlyOnce(actionFunc func(), completeFileName string) {
	installCompleteFile := filepath.Join(os.TempDir(), completeFileName)

	// skip if install already complete, no need to lock
	_, err := os.Stat(installCompleteFile)
	if err == nil {
		return
	}

	lock, err := lockfile.New(filepath.Join(os.TempDir(), completeFileName+".lck"))
	if err != nil {
		log.Fatalf("Cannot init artifact retrieval lock: %v", err)
	}

	for err = lock.TryLock(); err != nil; err = lock.TryLock() {
		if _, ok := err.(lockfile.TemporaryError); ok {
			time.Sleep(5 * time.Second)
			log.Printf("Worker %v waiting for artifact retrieval lock: %v", *id, lock)
		} else {
			log.Fatalf("Worker %v could not obtain artifact retrieval lock: %v", *id, err)
		}
	}
	defer lock.Unlock()

	// skip if install already complete
	_, err = os.Stat(installCompleteFile)
	if err == nil {
		return
	}

	// do the real work
	actionFunc()

	// mark install complete
	os.OpenFile(installCompleteFile, os.O_RDONLY|os.O_CREATE, 0666)

}
