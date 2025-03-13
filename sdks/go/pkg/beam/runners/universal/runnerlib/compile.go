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

// Package runnerlib contains utilities for submitting Go pipelines
// to a Beam model runner.
package runnerlib

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"sync/atomic"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

// IsWorkerCompatibleBinary returns the path to itself and true if running
// a linux-amd64 binary that can directly be used as a worker binary.
func IsWorkerCompatibleBinary() (string, bool) {
	return "", false
}

var unique int32

// CompileOpts are additional options for dynamic compiles of the local code
// for development purposes. Production runs should build the worker binary
// separately for the target environment.
// See https://beam.apache.org/documentation/sdks/go-cross-compilation/ for details.
type CompileOpts struct {
	OS, Arch string
}

// BuildTempWorkerBinary creates a local worker binary in the tmp directory
// for linux/amd64. Caller responsible for deleting the binary.
func BuildTempWorkerBinary(ctx context.Context, opts CompileOpts) (string, error) {
	id := atomic.AddInt32(&unique, 1)
	filename := filepath.Join(os.TempDir(), fmt.Sprintf("worker-%v-%v", id, time.Now().UnixNano()))
	if err := buildWorkerBinary(ctx, filename, opts); err != nil {
		return "", err
	}
	return filename, nil
}

// buildWorkerBinary creates a local worker binary for linux/amd64. It finds the filename
// by examining the call stack. We want the user entry (*), for example:
//
//	  /Users/herohde/go/src/github.com/apache/beam/sdks/go/pkg/beam/runners/beamexec/main.go (skip: 2)
//	* /Users/herohde/go/src/github.com/apache/beam/sdks/go/examples/wordcount/wordcount.go (skip: 3)
//	  /usr/local/go/src/runtime/proc.go (skip: 4)      // not always present
//	  /usr/local/go/src/runtime/asm_amd64.s (skip: 4 or 5)
func buildWorkerBinary(ctx context.Context, filename string, opts CompileOpts) error {
	program := ""
	var isTest bool
	for i := 3; ; i++ {
		_, file, _, ok := runtime.Caller(i)
		if !ok || !strings.HasSuffix(file, ".go") || strings.HasSuffix(file, "runtime/proc.go") {
			break
		} else if strings.HasSuffix(file, "testing/testing.go") {
			isTest = true
			break
		}
		program = file
	}
	if !strings.HasSuffix(program, ".go") {
		return errors.New("could not detect user main")
	}
	goos := "linux"
	goarch := "amd64"

	if opts.OS != "" {
		goos = opts.OS
	}
	if opts.Arch != "" {
		goarch = opts.Arch
	}

	cgo := "0"

	log.Infof(ctx, "Cross-compiling %v with GOOS=%s GOARCH=%s CGO_ENABLED=%s as %v", program, goos, goarch, cgo, filename)

	// Cross-compile given go program. Not awesome.
	program = program[:strings.LastIndex(program, "/")+1]
	program = program + "."
	var build []string
	if isTest {
		build = []string{"go", "test", "-trimpath", "-c", "-o", filename, program}
	} else {
		build = []string{"go", "build", "-trimpath", "-o", filename, program}
	}

	cmd := exec.Command(build[0], build[1:]...)
	cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch, "CGO_ENABLED="+cgo)
	if out, err := cmd.CombinedOutput(); err != nil {
		return errors.Errorf("failed to cross-compile %v, see https://beam.apache.org/documentation/sdks/go-cross-compilation/ for details: %v\n%v", program, err, string(out))
	}
	return nil
}
