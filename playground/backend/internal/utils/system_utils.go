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

package utils

import (
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/logger"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
)

const (
	executableFiles = "executable_files"
)

// GetFuncName returns the name of the received func
func GetFuncName(i interface{}) string {
	fullName := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
	splitName := strings.Split(fullName, ".")
	return splitName[len(splitName)-1]
}

// GetLivenessFunction returns the function for the liveness check of the server.
func GetLivenessFunction() func(writer http.ResponseWriter, request *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}
}

// GetReadinessFunction returns the function that checks the readiness of the server to process a new code processing request
func GetReadinessFunction(envs *environment.Environment) func(writer http.ResponseWriter, request *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		if CheckNumOfTheParallelJobs(envs.ApplicationEnvs.WorkingDir(), envs.BeamSdkEnvs.NumOfParallelJobs()) {
			writer.WriteHeader(http.StatusOK)
		} else {
			writer.WriteHeader(http.StatusLocked)
		}
	}
}

// CheckNumOfTheParallelJobs checks the number of currently working code executions.
//
//	It counts by the number of the /path/to/workingDir/executableFiles/{pipelineId} folders.
//
// If it is equals or more than numOfParallelJobs, then returns false.
// If it is less than numOfParallelJobs, then returns true.
func CheckNumOfTheParallelJobs(workingDir string, numOfParallelJobs int) bool {
	baseFileFolder := filepath.Join(workingDir, executableFiles)
	_, err := os.Stat(baseFileFolder)
	if os.IsNotExist(err) {
		return true
	}

	dirEntries, err := os.ReadDir(baseFileFolder)
	if err != nil {
		logger.Errorf("Readiness: Error during read %s: %s", baseFileFolder, err.Error())
		return false
	}

	if len(dirEntries) >= numOfParallelJobs {
		logger.Errorf("Readiness: Count of code processing is equal or more than possible: %d / %d", len(dirEntries), numOfParallelJobs)
		return false
	}
	return true
}
