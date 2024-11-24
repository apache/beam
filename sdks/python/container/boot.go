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
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/apache/beam/sdks/v2/go/container/tools"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/artifact"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/xlangx/expansionx"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/execx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/grpcx"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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
	flag.Parse()

	if *setupOnly {
		processArtifactsInSetupOnlyMode()
		os.Exit(0)
	}

	if *workerPool {
		workerPoolId := fmt.Sprintf("%d", os.Getpid())
		os.Setenv(workerPoolIdEnv, workerPoolId)
		args := []string{
			"-m",
			"apache_beam.runners.worker.worker_pool_main",
			"--service_port=50000",
			"--container_executable=/opt/apache/beam/boot",
		}
		log.Printf("Starting worker pool %v: python %v", workerPoolId, strings.Join(args, " "))
		pythonVersion, err := expansionx.GetPythonVersion()
		if err != nil {
			log.Fatalf("Python SDK worker pool exited with error: %v", err)
		}
		if err := execx.Execute(pythonVersion, args...); err != nil {
			log.Fatalf("Python SDK worker pool exited with error: %v", err)
		}
		log.Print("Python SDK worker pool exited.")
		os.Exit(0)
	}

	if *id == "" {
		log.Fatalf("No id provided.")
	}
	if *provisionEndpoint == "" {
		log.Fatalf("No provision endpoint provided.")
	}

	if err := launchSDKProcess(); err != nil {
		log.Fatal(err)
	}
}

func launchSDKProcess() error {
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
		log.Fatalf("No logging endpoint provided.")
	}
	if *artifactEndpoint == "" {
		log.Fatalf("No artifact endpoint provided.")
	}
	if *controlEndpoint == "" {
		log.Fatalf("No control endpoint provided.")
	}
	logger := &tools.Logger{Endpoint: *loggingEndpoint}
	logger.Printf(ctx, "Initializing python harness: %v", strings.Join(os.Args, " "))

	// (1) Obtain the pipeline options

	options, err := tools.ProtoToJSON(info.GetPipelineOptions())
	if err != nil {
		logger.Fatalf(ctx, "Failed to convert pipeline options: %v", err)
	}

	// (2) Retrieve and install the staged packages.
	//
	// No log.Fatalf() from here on, otherwise deferred cleanups will not be called!

	// Trap signals, so we can clean up properly.
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	// Create a separate virtual environment (with access to globally installed packages), unless disabled by the user.
	// This improves usability on runners that persist the execution environment for the boot entrypoint between multiple pipeline executions.
	if os.Getenv("RUN_PYTHON_SDK_IN_DEFAULT_ENVIRONMENT") == "" {
		venvDir, err := setupVenv(ctx, logger, "/opt/apache/beam-venv", *id)
		if err != nil {
			return errors.New(
				"failed to create a virtual environment. If running on Ubuntu systems, " +
					"you might need to install `python3-venv` package. " +
					"To run the SDK process in default environment instead, " +
					"set the environment variable `RUN_PYTHON_SDK_IN_DEFAULT_ENVIRONMENT=1`. " +
					"In custom Docker images, you can do that with an `ENV` statement. " +
					fmt.Sprintf("Encountered error: %v", err))
		}
		cleanupFunc := func() {
			os.RemoveAll(venvDir)
			logger.Printf(ctx, "Cleaned up temporary venv for worker %v.", *id)
		}
		defer cleanupFunc()
	}

	dir := filepath.Join(*semiPersistDir, "staged")
	files, err := artifact.Materialize(ctx, *artifactEndpoint, info.GetDependencies(), info.GetRetrievalToken(), dir)
	if err != nil {
		fmtErr := fmt.Errorf("failed to retrieve staged files: %v", err)
		// Send error message to logging service before returning up the call stack
		logger.Errorf(ctx, fmtErr.Error())
		// No need to fail the job if submission_environment_dependencies.txt cannot be loaded
		if strings.Contains(fmtErr.Error(), "submission_environment_dependencies.txt") {
			logger.Printf(ctx, "Ignore the error when loading submission_environment_dependencies.txt.")
		} else {
			return fmtErr
		}
	}

	// TODO(herohde): the packages to install should be specified explicitly. It
	// would also be possible to install the SDK in the Dockerfile.
	fileNames := make([]string, len(files))
	requirementsFiles := []string{requirementsFile}
	for i, v := range files {
		name, _ := artifact.MustExtractFilePayload(v)
		logger.Printf(ctx, "Found artifact: %s", name)
		fileNames[i] = name

		if v.RoleUrn == artifact.URNPipRequirementsFile {
			requirementsFiles = append(requirementsFiles, name)
		}
	}

	if setupErr := installSetupPackages(ctx, logger, fileNames, dir, requirementsFiles); setupErr != nil {
		fmtErr := fmt.Errorf("failed to install required packages: %v", setupErr)
		// Send error message to logging service before returning up the call stack
		logger.Errorf(ctx, fmtErr.Error())
		return fmtErr
	}

	// (3) Invoke python

	os.Setenv("PIPELINE_OPTIONS", options)
	os.Setenv("SEMI_PERSISTENT_DIRECTORY", *semiPersistDir)
	os.Setenv("LOGGING_API_SERVICE_DESCRIPTOR", (&pipepb.ApiServiceDescriptor{Url: *loggingEndpoint}).String())
	os.Setenv("CONTROL_API_SERVICE_DESCRIPTOR", (&pipepb.ApiServiceDescriptor{Url: *controlEndpoint}).String())
	os.Setenv("RUNNER_CAPABILITIES", strings.Join(info.GetRunnerCapabilities(), " "))

	if info.GetStatusEndpoint() != nil {
		os.Setenv("STATUS_API_SERVICE_DESCRIPTOR", info.GetStatusEndpoint().String())
	}

	if metadata := info.GetMetadata(); metadata != nil {
		if jobName, nameExists := metadata["job_name"]; nameExists {
			os.Setenv("JOB_NAME", jobName)
		}
		if jobID, idExists := metadata["job_id"]; idExists {
			os.Setenv("JOB_ID", jobID)
		}
	}

	workerIds := append([]string{*id}, info.GetSiblingWorkerIds()...)

	// Keep track of child PIDs for clean shutdown without zombies
	childPids := struct {
		v        []int
		canceled bool
		mu       sync.Mutex
	}{v: make([]int, 0, len(workerIds))}

	// Forward trapped signals to child process groups in order to terminate them gracefully and avoid zombies
	go func() {
		logger.Printf(ctx, "Received signal: %v", <-signalChannel)
		childPids.mu.Lock()
		childPids.canceled = true
		for _, pid := range childPids.v {
			go func(pid int) {
				// This goroutine will be canceled if the main process exits before the 5 seconds
				// have elapsed, i.e., as soon as all subprocesses have returned from Wait().
				time.Sleep(5 * time.Second)
				if err := syscall.Kill(-pid, syscall.SIGKILL); err == nil {
					logger.Warnf(ctx, "Worker process %v did not respond, killed it.", pid)
				}
			}(pid)
			syscall.Kill(-pid, syscall.SIGTERM)
		}
		childPids.mu.Unlock()
	}()

	args := []string{
		"-m",
		sdkHarnessEntrypoint,
	}

	var wg sync.WaitGroup
	wg.Add(len(workerIds))
	for _, workerId := range workerIds {
		go func(workerId string) {
			defer wg.Done()

			bufLogger := tools.NewBufferedLogger(logger)
			errorCount := 0
			for {
				childPids.mu.Lock()
				if childPids.canceled {
					childPids.mu.Unlock()
					return
				}
				logger.Printf(ctx, "Executing Python (worker %v): python %v", workerId, strings.Join(args, " "))
				cmd := StartCommandEnv(map[string]string{"WORKER_ID": workerId}, os.Stdin, bufLogger, bufLogger, "python", args...)
				childPids.v = append(childPids.v, cmd.Process.Pid)
				childPids.mu.Unlock()

				if err := cmd.Wait(); err != nil {
					// Retry on fatal errors, like OOMs and segfaults, not just
					// DoFns throwing exceptions.
					errorCount += 1
					bufLogger.FlushAtError(ctx)
					if errorCount < 4 {
						logger.Warnf(ctx, "Python (worker %v) exited %v times: %v\nrestarting SDK process",
							workerId, errorCount, err)
					} else {
						logger.Fatalf(ctx, "Python (worker %v) exited %v times: %v\nout of retries, failing container",
							workerId, errorCount, err)
					}
				} else {
					bufLogger.FlushAtDebug(ctx)
					logger.Printf(ctx, "Python (worker %v) exited.", workerId)
					break
				}
			}
		}(workerId)
	}
	wg.Wait()
	return nil
}

// Start a command object in a new process group with the given arguments with
// additional environment variables. It attaches stdio to the child process.
// Returns the process handle.
func StartCommandEnv(env map[string]string, stdin io.Reader, stdout, stderr io.Writer, prog string, args ...string) *exec.Cmd {
	cmd := exec.Command(prog, args...)
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if env != nil {
		cmd.Env = os.Environ()
		for k, v := range env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
	}

	// Create process group so we can clean up the whole subtree later without creating zombies
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: 0}
	cmd.Start()
	return cmd
}

// setupVenv initializes a local Python venv and sets the corresponding env variables
func setupVenv(ctx context.Context, logger *tools.Logger, baseDir, workerId string) (string, error) {
	dir := filepath.Join(baseDir, "beam-venv-worker-"+workerId)
	logger.Printf(ctx, "Initializing temporary Python venv in %v", dir)
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		// Probably leftovers from a previous run
		logger.Printf(ctx, "Cleaning up previous venv ...")
		if err := os.RemoveAll(dir); err != nil {
			return "", err
		}
	}
	if err := os.MkdirAll(dir, 0750); err != nil {
		return "", fmt.Errorf("failed to create Python venv directory: %s", err)
	}
	pythonVersion, err := expansionx.GetPythonVersion()
	if err != nil {
		return "", err
	}
	if err := execx.Execute(pythonVersion, "-m", "venv", "--system-site-packages", dir); err != nil {
		return "", fmt.Errorf("python venv initialization failed: %s", err)
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
	re := regexp.MustCompile(`Python (\d)\.(\d+).*`)
	pyVersions := re.FindStringSubmatch(string(stdoutStderr[:]))
	if len(pyVersions) != 3 {
		return fmt.Errorf("cannot get parse Python version from %s", stdoutStderr)
	}
	pyVersion := fmt.Sprintf("%s%s", pyVersions[1], pyVersions[2])
	wheelName := fmt.Sprintf("cp%s-cp%s-manylinux_2_17_x86_64.manylinux2014_x86_64.whl", pyVersion, pyVersion)
	acceptableWhlSpecs = append(acceptableWhlSpecs, wheelName)
	return nil
}

// installSetupPackages installs Beam SDK and user dependencies.
func installSetupPackages(ctx context.Context, logger *tools.Logger, files []string, workDir string, requirementsFiles []string) error {
	bufLogger := tools.NewBufferedLogger(logger)
	bufLogger.Printf(ctx, "Installing setup packages ...")

	if err := setupAcceptableWheelSpecs(); err != nil {
		bufLogger.Printf(ctx, "Failed to setup acceptable wheel specs, leave it as empty: %v", err)
	}

	// Install the Dataflow Python SDK if one was staged. In released
	// container images, SDK is already installed, but can be overriden
	// using the --sdk_location pipeline option.
	if err := installSdk(ctx, logger, files, workDir, sdkSrcFile, acceptableWhlSpecs, false); err != nil {
		return fmt.Errorf("failed to install SDK: %v", err)
	}
	pkgName := "apache-beam"
	isSdkInstalled := isPackageInstalled(pkgName)
	if !isSdkInstalled {
		return fmt.Errorf("Apache Beam is not installed in the runtime environment. If you use a custom container image, you must install apache-beam package in the custom image using same version of Beam as in the pipeline submission environment. For more information, see: the https://beam.apache.org/documentation/runtime/environments/")
	}
	// The staged files will not disappear due to restarts because workDir is a
	// folder that is mapped to the host (and therefore survives restarts).
	for _, f := range requirementsFiles {
		if err := pipInstallRequirements(ctx, logger, files, workDir, f); err != nil {
			return fmt.Errorf("failed to install requirements: %v", err)
		}
	}
	if err := installExtraPackages(ctx, logger, files, extraPackagesFile, workDir); err != nil {
		return fmt.Errorf("failed to install extra packages: %v", err)
	}
	if err := pipInstallPackage(ctx, logger, files, workDir, workflowFile, false, true, nil); err != nil {
		return fmt.Errorf("failed to install workflow: %v", err)
	}
	if err := logRuntimeDependencies(ctx, bufLogger); err != nil {
		bufLogger.Printf(ctx, "couldn't fetch the runtime python dependencies: %v", err)
	}
	if err := logSubmissionEnvDependencies(ctx, bufLogger, workDir); err != nil {
		bufLogger.Printf(ctx, "couldn't fetch the submission environment dependencies: %v", err)
	}

	return nil
}

// processArtifactsInSetupOnlyMode installs the dependencies found in artifacts
// when flag --setup_only and --artifacts exist. The setup mode will only
// process the provided artifacts and skip the actual worker program start up.
// The mode is useful for building new images with dependencies pre-installed so
// that the installation can be skipped at the pipeline runtime.
func processArtifactsInSetupOnlyMode() {
	if *artifacts == "" {
		log.Fatal("No --artifacts provided along with --setup_only flag.")
	}
	workDir := filepath.Dir(*artifacts)
	metadata, err := os.ReadFile(*artifacts)
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
		if err := protojson.Unmarshal([]byte(info), &artifactInformation); err != nil {
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
	if setupErr := installSetupPackages(context.Background(), nil, files, workDir, []string{requirementsFile}); setupErr != nil {
		log.Fatalf("Failed to install required packages: %v", setupErr)
	}
}

// logRuntimeDependencies logs the python dependencies
// installed in the runtime environment.
func logRuntimeDependencies(ctx context.Context, bufLogger *tools.BufferedLogger) error {
	pythonVersion, err := expansionx.GetPythonVersion()
	if err != nil {
		return err
	}
	bufLogger.Printf(ctx, "Using Python version:")
	args := []string{"--version"}
	if err := execx.ExecuteEnvWithIO(nil, os.Stdin, bufLogger, bufLogger, pythonVersion, args...); err != nil {
		bufLogger.FlushAtError(ctx)
	} else {
		bufLogger.FlushAtDebug(ctx)
	}
	bufLogger.Printf(ctx, "Logging runtime dependencies:")
	args = []string{"-m", "pip", "freeze"}
	if err := execx.ExecuteEnvWithIO(nil, os.Stdin, bufLogger, bufLogger, pythonVersion, args...); err != nil {
		bufLogger.FlushAtError(ctx)
	} else {
		bufLogger.FlushAtDebug(ctx)
	}
	return nil
}

// logSubmissionEnvDependencies logs the python dependencies
// installed in the submission environment.
func logSubmissionEnvDependencies(ctx context.Context, bufLogger *tools.BufferedLogger, dir string) error {
	bufLogger.Printf(ctx, "Logging submission environment dependencies:")
	// path for submission environment dependencies should match with the
	// one defined in apache_beam/runners/portability/stager.py.
	filename := filepath.Join(dir, "submission_environment_dependencies.txt")
	content, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	bufLogger.Printf(ctx, string(content))
	return nil
}
