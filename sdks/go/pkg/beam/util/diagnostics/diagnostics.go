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

// Package diagnostics is a beam internal package that contains code for writing and uploading
// diagnostic info (e.g. heap profiles) about the worker process.
// This package is not intended for end user use and can change at any time.
package diagnostics

import (
	"bufio"
	"context"
	"errors"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

const (
	hProfLoc     = "/tmp/hProf"
	tempHProfLoc = "/tmp/hProf.tmp"
)

// SampleForHeapProfile checks every second if it should take a heap profile, and if so
// it takes one and saves it to hProfLoc. A profile will be taken if either:
// (1) the amount of memory allocated has increased since the last heap profile was taken or
// (2) it has been 60 seconds since the last heap profile was taken
func SampleForHeapProfile(ctx context.Context, samplingFrequencySeconds, maxTimeBetweenDumpsSeconds int) {
	var maxAllocatedSoFar uint64
	samplesSkipped := 0
	for {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		if m.Alloc > maxAllocatedSoFar || (samplesSkipped+1)*samplingFrequencySeconds > maxTimeBetweenDumpsSeconds {
			samplesSkipped = 0
			maxAllocatedSoFar = m.Alloc
			err := saveHeapProfile()
			if err != nil {
				log.Warnf(ctx, "Failed to save heap profile. This will not affect pipeline execution, but may make it harder to diagnose memory issues: %v", err)
			}
		} else {
			samplesSkipped++
		}
		time.Sleep(time.Duration(samplingFrequencySeconds) * time.Second)
	}
}

func saveHeapProfile() error {
	// Write to a .tmp file before moving to final location to ensure
	// that OOM doesn't disrupt this flow.
	// Try removing temp file in case we ran into an error previously
	if err := os.Remove(tempHProfLoc); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	fd, err := os.Create(tempHProfLoc)
	if err != nil {
		return err
	}
	defer fd.Close()
	buf := bufio.NewWriterSize(fd, 1<<20) // use 1MB buffer

	if err := pprof.WriteHeapProfile(buf); err != nil {
		return err
	}

	if err := buf.Flush(); err != nil {
		return err
	}

	if err := os.Remove(hProfLoc); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return os.Rename(tempHProfLoc, hProfLoc)
}

// UploadHeapProfile checks if a heap profile is available and, if so, uploads it to dest.
// It will first check hProfLoc for the heap profile and then it will
// check tempHProfLoc if no file exists at hProfLoc.
//
// To examine, download the file and run: `go tool pprof -http=:8082 path/to/profile`
func UploadHeapProfile(ctx context.Context, dest string) error {
	hProf, err := os.Open(hProfLoc)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			hProf, err = os.Open(tempHProfLoc)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil
				}
				return err
			}
		}
	}
	defer hProf.Close()
	hProfReader := bufio.NewReader(hProf)

	fs, err := filesystem.New(ctx, dest)
	if err != nil {
		return err
	}
	defer fs.Close()
	fd, err := fs.OpenWrite(ctx, dest)
	if err != nil {
		return err
	}
	buf := bufio.NewWriterSize(fd, 1<<20) // use 1MB buffer

	_, err = hProfReader.WriteTo(buf)
	if err != nil {
		return err
	}

	if err := buf.Flush(); err != nil {
		return err
	}

	return fd.Close()
}
