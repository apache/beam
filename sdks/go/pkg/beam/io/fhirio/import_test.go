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

package fhirio

import (
	"os"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func setupTempAndDeadLetterDirs() (string, string, func()) {
	tempPath, _ := os.MkdirTemp("", "temp")
	deadLetterPath, _ := os.MkdirTemp("", "deadletter")
	return tempPath, deadLetterPath, func() {
		os.RemoveAll(tempPath)
		os.RemoveAll(deadLetterPath)
	}
}

func TestImport(t *testing.T) {
	testCases := []struct {
		name                     string
		client                   fhirStoreClient
		expectedErrorCount       int
		expectedSuccessCount     int
		expectedOperationResults operationResults
	}{
		{
			name:                     "Import returns error",
			client:                   requestReturnErrorFakeClient,
			expectedErrorCount:       1,
			expectedOperationResults: operationResults{},
		},
		{
			name: "Import returns successfully",
			client: &fakeFhirStoreClient{fakeImportResources: func(string, string, ContentStructure) (operationResults, error) {
				return testOperationResult, nil
			}},
			expectedErrorCount:       0,
			expectedOperationResults: testOperationResult,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			tempDirPath, deadLetterDirPath, teardownDirs := setupTempAndDeadLetterDirs()
			defer teardownDirs()

			testResources := []string{"foo", "bar"}
			p, s, testResourcesCol := ptest.CreateList(testResources)
			failedResources, deadLetter := importResourcesInBatches(
				s,
				"",
				tempDirPath,
				deadLetterDirPath,
				0,
				testResourcesCol,
				testCase.client,
			)

			passert.Empty(s, failedResources)
			passert.Count(s, deadLetter, "", testCase.expectedErrorCount)

			pipelineResult := ptest.RunAndValidate(t, p)
			validateCounter(t, pipelineResult, operationErrorCounterName, testCase.expectedErrorCount)
			validateCounter(t, pipelineResult, successCounterName, int(testCase.expectedOperationResults.Successes))
			validateCounter(t, pipelineResult, errorCounterName, int(testCase.expectedOperationResults.Failures))

			if filesInTemp, _ := os.ReadDir(tempDirPath); len(filesInTemp) != 0 {
				t.Fatalf("expected 0 files in temp path, but got %v", len(filesInTemp))
			}

			if filesInDeadLetter, _ := os.ReadDir(deadLetterDirPath); len(filesInDeadLetter) != testCase.expectedErrorCount {
				t.Fatalf("expected 1 file in deadletter path, but got %v", len(filesInDeadLetter))
			}
		})
	}
}
