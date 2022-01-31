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
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/artifact"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/provision"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/execx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/grpcx"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
)

var (
	acceptableWhlSpecs []string

	// SetupOnly option is used to invoke the boot sequence to only process the provided artifacts and builds new dependency pre-cached images.
	setupOnly = flag.Bool("setup_only", false, "Execute boot program in setup only mode (optional).")
	artifacts = flag.String("artifacts", "", "Path to artifacts metadata file used in setup only mode (optional).")

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

	standardArtifactFileTypeUrn = "beam:artifact:type:file:v1"
)

func main() {
	if err := mainError(); err != nil {
	    log.Print(err)
	    os.Exit(1)
	}
}

func mainError() error {
	flag.Parse()

	if *setupOnly {
		if err := processArtifactsInSetupOnlyMode(); err != nil {
			return fmt.Errorf("Setup unsuccessful with error: %v", err)
		}
		return nil
	}

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
		return fmt.Errorf("Python SDK worker pool exited: %v", execx.Execute("python", args...))
	}

	if *id == "" {
		return fmt.Errorf("No id provided.")
	}
	if *provisionEndpoint == "" {
		return fmt.Errorf("No provision endpoint provided.")
	}

	ctx := grpcx.WriteWorkerID(context.Background(), *id)

	info, err := provision.Info(ctx, *provisionEndpoint)
	if err != nil {
		return fmt.Errorf("Failed to obtain provisioning information: %v", err)
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
		return fmt.Errorf("No logging endpoint provided.")
	}
	if *artifactEndpoint == "" {
		return fmt.Errorf("No artifact endpoint provided.")
	}
	if *controlEndpoint == "" {
		return fmt.Errorf("No control endpoint provided.")
	}

	log.Printf("Initializing python harness: %v", strings.Join(os.Args, " "))

	// (1) Obtain the pipeline options

	options, err := provision.ProtoToJSON(info.GetPipelineOptions())
	if err != nil {
		return fmt.Errorf("Failed to convert pipeline options: %v", err)
	}

	// (2) Retrieve and install the staged packages.
	//
	// No log.Fatalf() from here on, otherwise deferred cleanups will not be called!

	venvDir, err := setupVenv(filepath.Join(*semiPersistDir, "beam-venv"), *id)
	if err != nil {
	    return fmt.Errorf("Failed to initialize Python venv.")
	}
	cleanupFunc := func() {
	    log.Printf("Cleaning up temporary venv ...")
	    os.RemoveAll(venvDir)
	}
	defer cleanupFunc()
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)
	go func() {
	    log.Printf("Received signal: ", (<-signalChannel).String())
	    cleanupFunc()
	    os.Exit(1)
	}()

	dir := filepath.Join(*semiPersistDir, "staged")

	files, err := artifact.Materialize(ctx, *artifactEndpoint, info.GetDependencies(), info.GetRetrievalToken(), dir)
	if err != nil {
	    return fmt.Errorf("Failed to retrieve staged files: %v", err)
	}

	// TODO(herohde): the packages to install should be specified explicitly. It
	// would also be possible to install the SDK in the Dockerfile.
	fileNames := make([]string, len(files))
	requirementsFiles := []string{requirementsFile}
	for i, v := range files {
	    name, _ := artifact.MustExtractFilePayload(v)
	    log.Printf("Found artifact: %s", name)
	    fileNames[i] = name

	    if v.RoleUrn == artifact.URNPipRequirementsFile {
	        requirementsFiles = append(requirementsFiles, name)
	    }
	}

	if setupErr := installSetupPackages(fileNames, dir, requirementsFiles); setupErr != nil {
	    return fmt.Errorf("Failed to install required packages: %v", setupErr)
	}

	// (3) Invoke python

	os.Setenv("PIPELINE_OPTIONS", options)
	os.Setenv("SEMI_PERSISTENT_DIRECTORY", *semiPersistDir)
	os.Setenv("LOGGING_API_SERVICE_DESCRIPTOR", proto.MarshalTextString(&pipepb.ApiServiceDescriptor{Url: *loggingEndpoint}))
	os.Setenv("CONTROL_API_SERVICE_DESCRIPTOR", proto.MarshalTextString(&pipepb.ApiServiceDescriptor{Url: *controlEndpoint}))
	os.Setenv("RUNNER_CAPABILITIES", strings.Join(info.GetRunnerCapabilities(), " "))

	if info.GetStatusEndpoint() != nil {
		os.Setenv("STATUS_API_SERVICE_DESCRIPTOR", proto.MarshalTextString(info.GetStatusEndpoint()))
	}

	if metadata := info.GetMetadata(); metadata != nil {
		if jobName, nameExists := metadata["job_name"]; nameExists {
			os.Setenv("JOB_NAME", jobName)
		}
		if jobID, idExists := metadata["job_id"]; idExists {
			os.Setenv("JOB_ID", jobID)
		}
	}

	args := []string{
		"-m",
		sdkHarnessEntrypoint,
	}

	workerIds := append([]string{*id}, info.GetSiblingWorkerIds()...)
	var wg sync.WaitGroup
	wg.Add(len(workerIds))
	for _, workerId := range workerIds {
		go func(workerId string) {
			defer wg.Done()
			log.Printf("Executing Python (worker %v): python %v", workerId, strings.Join(args, " "))
			log.Printf("Python (worker %v) exited with code: %v", workerId, execx.ExecuteEnv(map[string]string{"WORKER_ID": workerId}, "python", args...))
		}(workerId)
	}
	wg.Wait()

	return nil
}

// setupVenv initialize a local Python venv and set the corresponding env variables
func setupVenv(baseDir, workerId string) (string, error) {
	log.Printf("Initializing temporary Python venv ...")

	if err := os.MkdirAll(baseDir, 0750); err != nil {
	    return "", fmt.Errorf("Failed to create venv base directory: %s", err)
	}
	dir, err := ioutil.TempDir(baseDir, fmt.Sprintf("beam-venv-%s-", workerId))
	if err != nil {
	    return "", fmt.Errorf("Failed Python venv directory: %s", err)
	}
	args := []string{"-m", "venv", "--system-site-packages", dir}
	if err := execx.Execute("python", args...); err != nil {
	    return "", err
	}
	os.Setenv("VIRTUAL_ENV", dir)
	os.Setenv("PATH", strings.Join([]string{filepath.Join(dir, "bin"), os.Getenv("PATH")}, ":"))
	return dir, nil
}

// setupAcceptableWheelSpecs setup wheel specs according to installed python version
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
	case "36", "37":
		wheelName = fmt.Sprintf("cp%s-cp%sm-manylinux1_x86_64.whl", pyVersion, pyVersion)
	default:
		wheelName = fmt.Sprintf("cp%s-cp%s-manylinux1_x86_64.whl", pyVersion, pyVersion)
	}
	acceptableWhlSpecs = append(acceptableWhlSpecs, wheelName)
	return nil
}

// installSetupPackages installs Beam SDK and user dependencies.
func installSetupPackages(files []string, workDir string, requirementsFiles []string) error {
	log.Printf("Installing setup packages ...")

	if err := setupAcceptableWheelSpecs(); err != nil {
		log.Printf("Failed to setup acceptable wheel specs, leave it as empty: %v", err)
	}

	// Install the Dataflow Python SDK and worker packages.
	// We install the extra requirements in case of using the beam sdk. These are ignored by pip
	// if the user is using an SDK that does not provide these.
	if err := installSdk(files, workDir, sdkSrcFile, acceptableWhlSpecs, false); err != nil {
		return fmt.Errorf("failed to install SDK: %v", err)
	}
	// The staged files will not disappear due to restarts because workDir is a
	// folder that is mapped to the host (and therefore survives restarts).
	for _, f := range requirementsFiles {
		if err := pipInstallRequirements(files, workDir, f); err != nil {
			return fmt.Errorf("failed to install requirements: %v", err)
		}
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

// processArtifactsInSetupOnlyMode installs the dependencies found in artifacts
// when flag --setup_only and --artifacts exist. The setup mode will only
// process the provided artifacts and skip the actual worker program start up.
// The mode is useful for building new images with dependencies pre-installed so
// that the installation can be skipped at the pipeline runtime.
func processArtifactsInSetupOnlyMode() error {
	if *artifacts == "" {
		log.Fatal("No --artifacts provided along with --setup_only flag.")
	}
	workDir := filepath.Dir(*artifacts)
	metadata, err := ioutil.ReadFile(*artifacts)
	if err != nil {
		log.Fatalf("Unable to open artifacts metadata file %v with error %v", *artifacts, err)
	}
	var infoJsons []string
	if err := json.Unmarshal(metadata, &infoJsons); err != nil {
		log.Fatalf("Unable to parse metadata, error: %v", err)
	}

	files := make([]string, len(infoJsons))
	for i, info := range infoJsons {
		var artifactInformation pipepb.ArtifactInformation
		if err := jsonpb.UnmarshalString(info, &artifactInformation); err != nil {
			log.Fatalf("Unable to unmarshal artifact information from json string %v", info)
		}

		// For now we only expect artifacts in file type. The condition should be revisited if the assumption is not valid any more.
		if artifactInformation.GetTypeUrn() != standardArtifactFileTypeUrn {
			log.Fatalf("Expect file artifact type in setup only mode, found %v.", artifactInformation.GetTypeUrn())
		}
		filePayload := &pipepb.ArtifactFilePayload{}
		if err := proto.Unmarshal(artifactInformation.GetTypePayload(), filePayload); err != nil {
			log.Fatal("Unable to unmarshal artifact information type payload.")
		}
		files[i] = filePayload.GetPath()
	}
	if setupErr := installSetupPackages(files, workDir, []string{requirementsFile}); setupErr != nil {
		log.Fatalf("Failed to install required packages: %v", setupErr)
	}
	return nil
}
