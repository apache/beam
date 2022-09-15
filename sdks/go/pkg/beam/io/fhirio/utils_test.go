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
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"google.golang.org/api/healthcare/v1"
)

var (
	testOperationResult = operationResults{Successes: 5, Failures: 2}

	fakeRequestReturnErrorMessage = "internal error"
	requestReturnErrorFakeClient  = &fakeFhirStoreClient{
		fakeReadResources: func([]byte) (*http.Response, error) {
			return nil, errors.New(fakeRequestReturnErrorMessage)
		},
		fakeExecuteBundles: func(string, string) (*http.Response, error) {
			return nil, errors.New(fakeRequestReturnErrorMessage)
		},
		fakeSearch: func(string, string, map[string]string, string) (*http.Response, error) {
			return nil, errors.New(fakeRequestReturnErrorMessage)
		},
		fakeDeidentify: func(string, string, *healthcare.DeidentifyConfig) (operationResults, error) {
			return operationResults{}, errors.New(fakeRequestReturnErrorMessage)
		},
		fakeImportResources: func(string, string, ContentStructure) (operationResults, error) {
			return operationResults{}, errors.New(fakeRequestReturnErrorMessage)
		},
	}

	badStatusFakeResponse = &http.Response{
		Body:       io.NopCloser(bytes.NewBufferString("response")),
		StatusCode: http.StatusForbidden,
	}
	badStatusFakeClient = &fakeFhirStoreClient{
		fakeReadResources: func([]byte) (*http.Response, error) {
			return badStatusFakeResponse, nil
		},
		fakeExecuteBundles: func(string, string) (*http.Response, error) {
			return badStatusFakeResponse, nil
		},
		fakeSearch: func(string, string, map[string]string, string) (*http.Response, error) {
			return badStatusFakeResponse, nil
		},
	}

	fakeBodyReaderErrorMessage  = "ReadAll fail"
	bodyReaderErrorFakeResponse = &http.Response{
		Body: &fakeReaderCloser{
			fakeRead: func([]byte) (int, error) {
				return 0, errors.New(fakeBodyReaderErrorMessage)
			},
		},
		StatusCode: http.StatusOK,
	}
	bodyReaderErrorFakeClient = &fakeFhirStoreClient{
		fakeReadResources: func([]byte) (*http.Response, error) {
			return bodyReaderErrorFakeResponse, nil
		},
		fakeExecuteBundles: func(string, string) (*http.Response, error) {
			return bodyReaderErrorFakeResponse, nil
		},
		fakeSearch: func(string, string, map[string]string, string) (*http.Response, error) {
			return bodyReaderErrorFakeResponse, nil
		},
	}

	emptyBodyReaderFakeResponse = &http.Response{
		Body:       io.NopCloser(bytes.NewBuffer(nil)),
		StatusCode: http.StatusOK,
	}
	emptyResponseBodyFakeClient = &fakeFhirStoreClient{
		fakeExecuteBundles: func(string, string) (*http.Response, error) {
			return emptyBodyReaderFakeResponse, nil
		},
		fakeSearch: func(string, string, map[string]string, string) (*http.Response, error) {
			return emptyBodyReaderFakeResponse, nil
		},
	}
)

type fakeFhirStoreClient struct {
	fakeReadResources   func([]byte) (*http.Response, error)
	fakeExecuteBundles  func(string, string) (*http.Response, error)
	fakeSearch          func(string, string, map[string]string, string) (*http.Response, error)
	fakeDeidentify      func(string, string, *healthcare.DeidentifyConfig) (operationResults, error)
	fakeImportResources func(string, string, ContentStructure) (operationResults, error)
}

func (c *fakeFhirStoreClient) executeBundle(storePath, bundle string) (*http.Response, error) {
	return c.fakeExecuteBundles(storePath, bundle)
}

func (c *fakeFhirStoreClient) readResource(resourcePath []byte) (*http.Response, error) {
	return c.fakeReadResources(resourcePath)
}

func (c *fakeFhirStoreClient) search(storePath, resourceType string, queries map[string]string, pageToken string) (*http.Response, error) {
	return c.fakeSearch(storePath, resourceType, queries, pageToken)
}

func (c *fakeFhirStoreClient) deidentify(srcStorePath, dstStorePath string, deidConfig *healthcare.DeidentifyConfig) (operationResults, error) {
	return c.fakeDeidentify(srcStorePath, dstStorePath, deidConfig)
}

func (c *fakeFhirStoreClient) importResources(storePath, gcsURI string, contentStructure ContentStructure) (operationResults, error) {
	return c.fakeImportResources(storePath, gcsURI, contentStructure)
}

// Useful to fake the Body of a http.Response.
type fakeReaderCloser struct {
	io.Closer
	fakeRead func([]byte) (int, error)
}

func (m *fakeReaderCloser) Read(b []byte) (int, error) {
	return m.fakeRead(b)
}

func validateCounter(t *testing.T, pipelineResult beam.PipelineResult, expectedCounterName string, expectedCount int) {
	t.Helper()

	counterResults := pipelineResult.Metrics().Query(func(mr beam.MetricResult) bool {
		return mr.Name() == expectedCounterName
	}).Counters()

	if expectedCount == 0 && len(counterResults) == 0 {
		return
	}

	if len(counterResults) != 1 {
		t.Fatalf("got %v counters with name %v, expected 1", len(counterResults), expectedCounterName)
	}
	counterResult := counterResults[0]

	if counterResult.Result() != int64(expectedCount) {
		t.Fatalf("counter %v result is %v, expected %v", expectedCounterName, counterResult.Result(), expectedCount)
	}
}
