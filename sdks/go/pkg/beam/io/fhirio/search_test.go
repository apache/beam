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
	"bytes"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestSearch_Errors(t *testing.T) {
	testCases := []struct {
		name           string
		client         fhirStoreClient
		containedError string
	}{
		{
			name:           "Search request returns error",
			client:         requestReturnErrorFakeClient,
			containedError: fakeRequestReturnErrorMessage,
		},
		{
			name:           "Search request returns bad status",
			client:         badStatusFakeClient,
			containedError: strconv.Itoa(http.StatusForbidden),
		},
		{
			name:           "Search request response body fails to be read",
			client:         bodyReaderErrorFakeClient,
			containedError: fakeBodyReaderErrorMessage,
		},
		{
			name:           "Search request response body failed to be decoded",
			client:         emptyResponseBodyFakeClient,
			containedError: io.EOF.Error(),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			p, s, testSearchQueryPCollection := ptest.CreateList([]SearchQuery{{}})
			resources, failedSearches := search(s, "any", testSearchQueryPCollection, testCase.client)
			passert.Empty(s, resources)
			passert.Count(s, failedSearches, "", 1)
			passert.True(s, failedSearches, func(errorMsg string) bool {
				return strings.Contains(errorMsg, testCase.containedError)
			})
			pipelineResult := ptest.RunAndValidate(t, p)
			validateCounter(t, pipelineResult, errorCounterName, 1)
			validateCounter(t, pipelineResult, successCounterName, 0)
		})
	}
}

func TestSearch_Pagination(t *testing.T) {
	paginationFakeClient := &fakeFhirStoreClient{
		fakeSearch: func(s, s2 string, m map[string]string, pageToken string) (*http.Response, error) {
			if pageToken == "theNextPageToken" {
				return &http.Response{
					Body:       io.NopCloser(bytes.NewBufferString(`{"entry": [{"resource":{"resourceType": "Patient", "id": "2"}}], "link": []}`)),
					StatusCode: http.StatusOK,
				}, nil
			} else {
				return &http.Response{
					Body:       io.NopCloser(bytes.NewBufferString(`{"entry": [{"resource":{"resourceType": "Patient", "id": "1"}}], "link": [{"relation":"next", "url":"https://healthcare.googleapis.com?_page_token=theNextPageToken"}]}`)),
					StatusCode: http.StatusOK,
				}, nil
			}
		},
	}
	p, s, testSearchQuery := ptest.CreateList([]SearchQuery{{}})
	resourcesFound, failedSearches := search(s, "any", testSearchQuery, paginationFakeClient)
	passert.Empty(s, failedSearches)
	passert.Count(s, resourcesFound, "", 1)
	resourcesFoundWithoutIdentifier := beam.DropKey(s, resourcesFound)
	passert.True(s, resourcesFoundWithoutIdentifier, func(resourcesFound []string) bool {
		return len(resourcesFound) == 2
	})
	pipelineResult := ptest.RunAndValidate(t, p)
	validateCounter(t, pipelineResult, successCounterName, 1)
}
