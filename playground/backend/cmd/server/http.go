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
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/logger"
	"context"
	"net/http"
	"os"
	"path/filepath"
)

// listenHttp binds the http.Handler on the TCP network address
func listenHttp(ctx context.Context, errChan chan error, envs *environment.Environment, handler http.Handler) {
	address := envs.NetworkEnvs.Address()
	logger.Infof("listening HTTP at %s\n", address)

	mux := http.NewServeMux()
	mux.Handle("/", handler)
	mux.HandleFunc("/readiness", func(w http.ResponseWriter, r *http.Request) {
		workingDir := envs.ApplicationEnvs.WorkingDir()
		countOfPossibleCodeProcessing := envs.BeamSdkEnvs.CountOfPossibleCodeProcessing()
		if isReady(workingDir, countOfPossibleCodeProcessing) {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusLocked)
		}
	})

	if err := http.ListenAndServe(address, mux); err != nil {
		errChan <- err
		return
	}
	for {
		<-ctx.Done()
		return
	}
}

// isReady checks the number of already working code processing.
//  It counts by the number of the /path/to/workingDir/executable_files/{pipelineId} folders.
// If it is equals or more than countOfPossibleCodeProcessing, then returns false.
// If it is less than countOfPossibleCodeProcessing, then returns true.
func isReady(workingDir string, countOfPossibleCodeProcessing int) bool {
	// TODO add getting of dir executable_files from environments.
	baseFileFolder := filepath.Join(workingDir, "executable_files")
	_, err := os.Stat(baseFileFolder)
	if os.IsNotExist(err) {
		return true
	}

	dirEntries, err := os.ReadDir(baseFileFolder)
	if err != nil {
		logger.Errorf("Readiness: Error during read %s: %s", baseFileFolder, err.Error())
		return false
	}

	if len(dirEntries) >= countOfPossibleCodeProcessing {
		logger.Errorf("Readiness: Count of code processing is equals or more than possible: %d / %d", len(dirEntries), countOfPossibleCodeProcessing)
		return false
	}
	return true
}
