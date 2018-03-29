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

	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

// BuildTempWorkerBinary creates a local worker binary in the tmp directory
// for linux/amd64. Caller responsible for deleting the binary.
func BuildTempWorkerBinary(ctx context.Context) (string, error) {
	filename := filepath.Join(os.TempDir(), fmt.Sprintf("beam-go-%v", time.Now().UnixNano()))
	if err := BuildWorkerBinary(ctx, filename); err != nil {
		return "", err
	}
	return filename, nil
}

// BuildWorkerBinary creates a local worker binary for linux/amd64. It finds the filename
// by examining the call stack. We want the user entry (*), for example:
//
//   /Users/herohde/go/src/github.com/apache/beam/sdks/go/pkg/beam/runners/beamexec/main.go (skip: 2)
// * /Users/herohde/go/src/github.com/apache/beam/sdks/go/examples/wordcount/wordcount.go (skip: 3)
//   /usr/local/go/src/runtime/proc.go (skip: 4)
//   /usr/local/go/src/runtime/asm_amd64.s (skip: 5)
func BuildWorkerBinary(ctx context.Context, filename string) error {
	program := ""
	for i := 3; ; i++ {
		_, file, _, ok := runtime.Caller(i)
		if !ok || strings.HasSuffix(file, "runtime/proc.go") {
			break
		}
		program = file
	}
	if program == "" {
		return fmt.Errorf("could not detect user main")
	}

	log.Infof(ctx, "Cross-compiling %v as %v", program, filename)

	// Cross-compile given go program. Not awesome.
	build := []string{"go", "build", "-o", filename, program}

	cmd := exec.Command(build[0], build[1:]...)
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to cross-compile %v: %v\n%v", program, err, out)
	}
	return nil
}
