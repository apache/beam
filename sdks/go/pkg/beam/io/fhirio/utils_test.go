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
	"fmt"
	"net/http"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

var (
	fakeRequestReturnErrorMessage = "internal error"
	requestReturnErrorFakeClient  = &fakeFhirStoreClient{
		fakeReadResources: func(resource string) (*http.Response, error) {
			return nil, errors.New(fakeRequestReturnErrorMessage)
		},
		fakeExecuteBundles: func(storePath string, bundle []byte) (*http.Response, error) {
			return nil, errors.New(fakeRequestReturnErrorMessage)
		},
	}

	fakeBadStatus         = "403 Forbidden"
	badStatusFakeResponse = &http.Response{Status: fakeBadStatus}
	badStatusFakeClient   = &fakeFhirStoreClient{
		fakeReadResources: func(resource string) (*http.Response, error) {
			return badStatusFakeResponse, nil
		},
		fakeExecuteBundles: func(storePath string, bundle []byte) (*http.Response, error) {
			return badStatusFakeResponse, nil
		},
	}

	fakeBodyReaderErrorMessage  = "ReadAll fail"
	bodyReaderErrorFakeResponse = &http.Response{
		Body: &fakeReaderCloser{
			fakeRead: func([]byte) (int, error) {
				return 0, errors.New(fakeBodyReaderErrorMessage)
			},
		}, Status: "200 Ok"}
	bodyReaderErrorFakeClient = &fakeFhirStoreClient{
		fakeReadResources: func(resource string) (*http.Response, error) {
			return bodyReaderErrorFakeResponse, nil
		},
		fakeExecuteBundles: func(storePath string, bundle []byte) (*http.Response, error) {
			return bodyReaderErrorFakeResponse, nil
		},
	}
)

type fakeFhirStoreClient struct {
	fakeReadResources  func(string) (*http.Response, error)
	fakeExecuteBundles func(storePath string, bundle []byte) (*http.Response, error)
}

func (c *fakeFhirStoreClient) executeBundle(storePath string, bundle []byte) (*http.Response, error) {
	return c.fakeExecuteBundles(storePath, bundle)
}

func (c *fakeFhirStoreClient) readResource(resourcePath string) (*http.Response, error) {
	return c.fakeReadResources(resourcePath)
}

// Useful to fake the Body of a http.Response.
type fakeReaderCloser struct {
	fakeRead func([]byte) (int, error)
}

func (*fakeReaderCloser) Close() error {
	return nil
}

func (m *fakeReaderCloser) Read(b []byte) (int, error) {
	return m.fakeRead(b)
}

func validateResourceErrorCounter(pipelineResult beam.PipelineResult, expectedCount int) error {
	counterResults := pipelineResult.Metrics().AllMetrics().Counters()
	if len(counterResults) != 1 {
		return fmt.Errorf("counterResults got length %v, expected %v", len(counterResults), 1)
	}
	counterResult := counterResults[0]

	expectedCounterName := "fhirio/resource_error_count"
	if counterResult.Name() != expectedCounterName {
		return fmt.Errorf("counterResult.Name() is '%v', expected '%v'", counterResult.Name(), expectedCounterName)
	}

	if counterResult.Result() != int64(expectedCount) {
		return fmt.Errorf("counterResult.Result() is %v, expected %v", counterResult.Result(), expectedCount)
	}
	return nil
}
