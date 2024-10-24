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

// Package init contains the harness initialization code defined by the FnAPI.
// It is a separate package to avoid flags in the harness package and must be
// imported by the runner to ensure the init hook is registered.
package init

import (
	"context"
	"encoding/json"
	"flag"
	"strings"
	"time"

	"fmt"
	"os"
	"runtime/debug"

	"golang.org/x/exp/slices"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/harness"

	// Import gcs filesystem so that it can be used to upload heap dumps.
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/gcs"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/grpcx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/syscallx"
)

var (
	// These flags handle the invocation by the container boot code.

	worker = flag.Bool("worker", false, "Whether binary is running in worker mode.")

	id              = flag.String("id", "", "Local identifier (required in worker mode).")
	loggingEndpoint = flag.String("logging_endpoint", "", "Local logging gRPC endpoint (required in worker mode).")
	controlEndpoint = flag.String("control_endpoint", "", "Local control gRPC endpoint (required in worker mode).")
	//lint:ignore U1000 semiPersistDir flag is passed in through the boot container, will need to be removed later
	semiPersistDir = flag.String("semi_persist_dir", "/tmp", "Local semi-persistent directory (optional in worker mode).")
	options        = flag.String("options", "", "JSON-encoded pipeline options (required in worker mode). (deprecated)")
)

type exitMode int

const (
	// Terminate means the hook should exit itself when the worker harness returns.
	Terminate exitMode = iota
	// Return means that the hook should return out, and allow the calling code to
	// determine if and when the process exits.
	// This may cause errors that caused worker failure to be ignored.
	Return
)

var (
	// ShutdownMode allows the runner to set how the worker harness should exit.
	ShutdownMode = Terminate
)

func init() {
	runtime.RegisterInit(hook)
}

// hook starts the harness, if in worker mode. Otherwise, is a no-op.
func hook() {
	if !*worker {
		return
	}

	// Extract environment variables. These are optional runner supported capabilities.
	// Expected env variables:
	// RUNNER_CAPABILITIES : list of runner supported capability urn.
	// STATUS_ENDPOINT : Endpoint to connect to status server used for worker status reporting.
	statusEndpoint := os.Getenv("STATUS_ENDPOINT")
	runnerCapabilities := strings.Split(os.Getenv("RUNNER_CAPABILITIES"), " ")

	// Initialization logging
	//
	// We use direct output to stderr here, because it is expected that logging
	// will be captured by the framework -- which may not be functional if
	// harness.Main returns. We want to be sure any error makes it out.

	pipelineOptionsFilename := os.Getenv("PIPELINE_OPTIONS_FILE")
	if pipelineOptionsFilename != "" {
		if *options != "" {
			fmt.Fprintf(os.Stderr, "WARNING: env variable PIPELINE_OPTIONS_FILE set but options flag populated. Potentially bad container loader. Flag value before overwrite: %v\n", options)
		}
		contents, err := os.ReadFile(pipelineOptionsFilename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read pipeline options file '%v': %v\n", pipelineOptionsFilename, err)
			os.Exit(1)
		}
		// Overwite flag to be consistent with the legacy flag processing.
		*options = string(contents)
	}
	// Load in pipeline options from the flag string. Used for both the new options file path
	// and the older flag approach.
	if *options != "" {
		var opt runtime.RawOptionsWrapper
		if err := json.Unmarshal([]byte(*options), &opt); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse pipeline options '%v': %v\n", *options, err)
			os.Exit(1)
		}
		runtime.GlobalOptions.Import(opt.Options)
		var experiments []string
		if e, ok := opt.Options.Options["experiments"]; ok {
			experiments = strings.Split(e, ",")
		}
		// TODO(zechenj18) 2023-12-07: Remove once the data sampling URN is properly sent in via the capabilities
		if slices.Contains(experiments, "enable_data_sampling") {
			runnerCapabilities = append(runnerCapabilities, graphx.URNDataSampling)
		}
	}

	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "Worker panic: %v\n", r)
			debug.PrintStack()
			switch ShutdownMode {
			case Terminate:
				os.Exit(2)
			case Return:
				return
			default:
				panic(fmt.Sprintf("unknown ShutdownMode: %v", ShutdownMode))
			}
		}
	}()

	// Since Init() is hijacking main, it's appropriate to do as main
	// does, and establish the background context here.
	// We produce a cancelFn here so runs in Loopback mode and similar can clean up
	// any leftover goroutines.
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	ctx = grpcx.WriteWorkerID(ctx, *id)
	memLimit := memoryLimit()
	if err := syscallx.SetProcessMemoryCeiling(memLimit, memLimit); err != nil && err != syscallx.ErrUnsupported {
		fmt.Println("Error Setting Rlimit ", err)
	}

	options := harness.Options{
		StatusEndpoint:     statusEndpoint,
		RunnerCapabilities: runnerCapabilities,
	}
	if err := harness.MainWithOptions(ctx, *loggingEndpoint, *controlEndpoint, options); err != nil {
		fmt.Fprintf(os.Stderr, "Worker failed: %v\n", err)
		switch ShutdownMode {
		case Terminate:
			os.Exit(1)
		case Return:
			return
		default:
			panic(fmt.Sprintf("unknown ShutdownMode: %v", ShutdownMode))
		}
	}
	fmt.Fprintln(os.Stderr, "Worker exited successfully!")
	for {
		// Just hang around until we're terminated.
		time.Sleep(time.Hour)
	}
}

// memoryLimits returns 90% of the physical memory on the machine. If it cannot determine
// that value, it returns 2GB. This is an imperfect heuristic. It aims to
// ensure there is enough memory for the process without causing an OOM.
func memoryLimit() uint64 {
	if size, err := syscallx.PhysicalMemorySize(); err == nil {
		return (size * 90) / 100
	}
	return 2 << 30
}
