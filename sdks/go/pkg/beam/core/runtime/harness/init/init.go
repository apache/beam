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
	"time"

	"fmt"
	"os"

	"runtime/debug"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/harness"
)

var (
	// These flags handle the invocation by the container boot code.

	worker = flag.Bool("worker", false, "Whether binary is running in worker mode.")

	id              = flag.String("id", "", "Local identifier (required in worker mode).")
	loggingEndpoint = flag.String("logging_endpoint", "", "Local logging gRPC endpoint (required in worker mode).")
	controlEndpoint = flag.String("control_endpoint", "", "Local control gRPC endpoint (required in worker mode).")
	semiPersistDir  = flag.String("semi_persist_dir", "/tmp", "Local semi-persistent directory (optional in worker mode).")
	options         = flag.String("options", "", "JSON-encoded pipeline options (required in worker mode).")
)

func init() {
	runtime.RegisterInit(hook)
}

// hook starts the harness, if in worker mode. Otherwise, is is a no-op.
func hook() {
	if !*worker {
		return
	}

	// Initialization logging
	//
	// We use direct output to stderr here, because it is expected that logging
	// will be captured by the framework -- which may not be functional if
	// harness.Main returns. We want to be sure any error makes it out.

	if *options != "" {
		var opt runtime.RawOptionsWrapper
		if err := json.Unmarshal([]byte(*options), &opt); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse pipeline options '%v': %v", *options, err)
			os.Exit(1)
		}
		runtime.GlobalOptions.Import(opt.Options)
	}

	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "Worker panic: %v", r)
			debug.PrintStack()
			os.Exit(2)
		}
	}()

	// Since Init() is hijacking main, it's appropriate to do as main
	// does, and establish the background context here.

	if err := harness.Main(context.Background(), *loggingEndpoint, *controlEndpoint); err != nil {
		fmt.Fprintf(os.Stderr, "Worker failed: %v", err)
		os.Exit(1)
	}

	fmt.Fprint(os.Stderr, "Worker exited successfully!")
	for {
		// Just hang around until we're terminated.
		time.Sleep(time.Hour)
	}
}
