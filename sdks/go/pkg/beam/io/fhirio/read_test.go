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
	"net/http"
	"strconv"
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
			name:           "Read request returns error",
			client:         requestReturnErrorFakeClient,
			containedError: fakeRequestReturnErrorMessage,
		},
		{
			name:           "Read request returns bad status",
			client:         badStatusFakeClient,
			containedError: strconv.Itoa(http.StatusForbidden),
		},
		{
			name:           "Read request response body fails to be read",
			client:         bodyReaderErrorFakeClient,
			containedError: fakeBodyReaderErrorMessage,
		},
	}

	testResourcePaths := [][]byte{[]byte("foo"), []byte("bar")}
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
			validateCounter(t, pipelineResult, errorCounterName, len(testResourcePaths))
			validateCounter(t, pipelineResult, successCounterName, 0)
		})
	}
}
