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
	"fmt"
	"io"
	"net/http"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

var (
	fakeRequestReturnErrorMessage = "internal error"
	requestReturnErrorFakeClient  = &fakeFhirStoreClient{
		fakeReadResources: func(string) (*http.Response, error) {
			return nil, errors.New(fakeRequestReturnErrorMessage)
		},
		fakeExecuteBundles: func(string, []byte) (*http.Response, error) {
			return nil, errors.New(fakeRequestReturnErrorMessage)
		},
		fakeSearch: func(string, string, map[string]string, string) (*http.Response, error) {
			return nil, errors.New(fakeRequestReturnErrorMessage)
		},
	}

	badStatusFakeResponse = &http.Response{
		Body:       io.NopCloser(bytes.NewBufferString("response")),
		StatusCode: http.StatusForbidden,
	}
	badStatusFakeClient = &fakeFhirStoreClient{
		fakeReadResources: func(string) (*http.Response, error) {
			return badStatusFakeResponse, nil
		},
		fakeExecuteBundles: func(string, []byte) (*http.Response, error) {
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
		fakeReadResources: func(string) (*http.Response, error) {
			return bodyReaderErrorFakeResponse, nil
		},
		fakeExecuteBundles: func(string, []byte) (*http.Response, error) {
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
		fakeExecuteBundles: func(string, []byte) (*http.Response, error) {
			return emptyBodyReaderFakeResponse, nil
		},
		fakeSearch: func(string, string, map[string]string, string) (*http.Response, error) {
			return emptyBodyReaderFakeResponse, nil
		},
	}
)

type fakeFhirStoreClient struct {
	fakeReadResources  func(string) (*http.Response, error)
	fakeExecuteBundles func(string, []byte) (*http.Response, error)
	fakeSearch         func(string, string, map[string]string, string) (*http.Response, error)
}

func (c *fakeFhirStoreClient) executeBundle(storePath string, bundle []byte) (*http.Response, error) {
	return c.fakeExecuteBundles(storePath, bundle)
}

func (c *fakeFhirStoreClient) readResource(resourcePath string) (*http.Response, error) {
	return c.fakeReadResources(resourcePath)
}

func (c *fakeFhirStoreClient) search(storePath, resourceType string, queries map[string]string, pageToken string) (*http.Response, error) {
	return c.fakeSearch(storePath, resourceType, queries, pageToken)
}

// Useful to fake the Body of a http.Response.
type fakeReaderCloser struct {
	io.Closer
	fakeRead func([]byte) (int, error)
}

func (m *fakeReaderCloser) Read(b []byte) (int, error) {
	return m.fakeRead(b)
}

func validateCounter(pipelineResult beam.PipelineResult, expectedCounterName string, expectedCount int) error {
	counterResults := pipelineResult.Metrics().AllMetrics().Counters()
	if len(counterResults) != 1 {
		return fmt.Errorf("counterResults got length %v, expected %v", len(counterResults), 1)
	}
	counterResult := counterResults[0]

	if counterResult.Name() != expectedCounterName {
		return fmt.Errorf("counterResult.Name() is '%v', expected '%v'", counterResult.Name(), expectedCounterName)
	}

	if counterResult.Result() != int64(expectedCount) {
		return fmt.Errorf("counterResult.Result() is %v, expected %v", counterResult.Result(), expectedCount)
	}
	return nil
}
