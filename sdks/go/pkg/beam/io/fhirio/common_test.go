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
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/healthcare/v1"
)

func TestPollTilCompleteAndCollectResults(t *testing.T) {
	testCases := []struct {
		name           string
		operation      *healthcare.Operation
		expectedResult operationResults
		expectedError  bool
	}{
		{
			name:           "Operation contains error",
			operation:      &healthcare.Operation{Done: true, Error: &healthcare.Status{Message: "internal error"}},
			expectedResult: operationResults{},
			expectedError:  true,
		},
		{
			name:           "Error bad json",
			operation:      &healthcare.Operation{Done: true, Metadata: []byte("bad json")},
			expectedResult: operationResults{},
			expectedError:  true,
		},
		{
			name:           "Success with 1 counter set",
			operation:      &healthcare.Operation{Done: true, Metadata: []byte(`{"counter":{"success":"4"}}`)},
			expectedResult: operationResults{Successes: 4, Failures: 0},
			expectedError:  false,
		},
		{
			name:           "Success with both counters set",
			operation:      &healthcare.Operation{Done: true, Metadata: []byte(`{"counter":{"success":"1","failure": "3"}}`)},
			expectedResult: operationResults{Successes: 1, Failures: 3},
			expectedError:  false,
		},
		{
			name:           "Success with no counters set",
			operation:      &healthcare.Operation{Done: true, Metadata: []byte(`{"counter":{}}`)},
			expectedResult: operationResults{Successes: 0, Failures: 0},
			expectedError:  false,
		},
	}

	client := &fhirStoreClientImpl{}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			operationResult, err := client.pollTilCompleteAndCollectResults(testCase.operation)
			if err != nil && !testCase.expectedError {
				t.Fatalf("Got unexpected error [%v]", err)
			}
			if !cmp.Equal(operationResult, testCase.expectedResult) {
				t.Fatalf("operationResult got %v, expected %v", operationResult, testCase.expectedResult)
			}
		})
	}
}
