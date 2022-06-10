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
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestRead(t *testing.T) {
	testCases := []struct {
		name           string
		client         fhirStoreClient
		containedError string
	}{
		{
			name: "Read Request Failed",
			client: &fakeFhirStoreClient{
				fakeReadResources: func(resource string) (*http.Response, error) {
					return nil, errors.New("")
				},
			},
			containedError: "failed fetching resource",
		},
		{
			name: "Read Request Returns Bad Status",
			client: &fakeFhirStoreClient{
				fakeReadResources: func(resource string) (*http.Response, error) {
					return &http.Response{StatusCode: 403}, nil
				},
			},
			containedError: "returned bad status",
		},
		{
			name: "Response body fails to be parsed",
			client: &fakeFhirStoreClient{
				fakeReadResources: func(resource string) (*http.Response, error) {
					return &http.Response{Body: &fakeReaderCloser{
						fakeRead: func([]byte) (int, error) {
							return 0, errors.New("")
						},
					}, StatusCode: 200}, nil
				},
			},
			containedError: "error reading response body",
		},
	}

	testResourcePaths := []string{"foo", "bar"}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			p, s, resourcePaths := ptest.CreateList(testResourcePaths)
			resources, failedReads := read(s, resourcePaths, testCase.client)
			passert.Empty(s, resources)
			passert.Count(s, failedReads, "", len(testResourcePaths))
			passert.True(s, failedReads, func(errorMsg string) bool {
				return strings.Contains(errorMsg, testCase.containedError)
			})
			pipelineResult := ptest.RunAndValidate(t, p)
			counterResults := pipelineResult.Metrics().AllMetrics().Counters()
			if len(counterResults) != 1 {
				t.Fatal("Only one counter should have been used")
			}
			if counterResults[0].Name() != "fhirio/read_resource_error_count" {
				t.Fatal("Only error counter should have been used")
			}
			if counterResults[0].Result() != int64(len(testResourcePaths)) {
				t.Fatal("Counter should have been incremented by the number of test resource paths")
			}
		})
	}
}
