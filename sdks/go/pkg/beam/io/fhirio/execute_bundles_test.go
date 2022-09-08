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
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestExecuteBundles(t *testing.T) {
	testCases := []struct {
		name           string
		client         fhirStoreClient
		containedError string
	}{
		{
			name:           "Execute Bundles request returns error",
			client:         requestReturnErrorFakeClient,
			containedError: fakeRequestReturnErrorMessage,
		},
		{
			name:           "Execute Bundles request returns bad status",
			client:         badStatusFakeClient,
			containedError: strconv.Itoa(http.StatusForbidden),
		},
		{
			name:           "Execute Bundles request response body fails to be read",
			client:         bodyReaderErrorFakeClient,
			containedError: fakeBodyReaderErrorMessage,
		},
		{
			name:           "Execute Bundles request response body failed to be decoded",
			client:         emptyResponseBodyFakeClient,
			containedError: io.EOF.Error(),
		},
	}

	testBundles := []string{"foo", "bar"}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			p, s, bundles := ptest.CreateList(testBundles)
			successfulBodies, failures := executeBundles(s, "bla", bundles, testCase.client)
			passert.Empty(s, successfulBodies)
			passert.Count(s, failures, "", len(testBundles))
			passert.True(s, failures, func(errorMsg string) bool {
				return strings.Contains(errorMsg, testCase.containedError)
			})
			pipelineResult := ptest.RunAndValidate(t, p)
			validateCounter(t, pipelineResult, errorCounterName, len(testBundles))
			validateCounter(t, pipelineResult, successCounterName, 0)
		})
	}
}
