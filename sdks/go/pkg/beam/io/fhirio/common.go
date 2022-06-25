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

// Package fhirio provides an API for reading and writing resources to Google
// Cloud Healthcare Fhir stores.
// Experimental.
package fhirio

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"regexp"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"google.golang.org/api/healthcare/v1"
	"google.golang.org/api/option"
)

const (
	UserAgent        = "apache-beam-io-google-cloud-platform-healthcare/" + core.SdkVersion
	baseMetricPrefix = "fhirio/"
)

func executeRequestAndRecordLatency(ctx context.Context, latencyMs *beam.Distribution, requestSupplier func() (*http.Response, error)) (*http.Response, error) {
	timeBeforeReadRequest := time.Now()
	response, err := requestSupplier()
	latencyMs.Update(ctx, time.Since(timeBeforeReadRequest).Milliseconds())
	return response, err
}

func extractBodyFrom(response *http.Response) (string, error) {
	if isBadStatusCode(response.Status) {
		return "", errors.Errorf("response contains bad status: [%v]", response.Status)
	}

	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}

	return string(bodyBytes), nil
}

func isBadStatusCode(status string) bool {
	// 2XXs are successes, otherwise failure.
	isMatch, err := regexp.MatchString("^2\\d{2}", status)
	if err != nil {
		return true
	}
	return !isMatch
}

type fhirioFnCommon struct {
	client                fhirStoreClient
	resourcesErrorCount   beam.Counter
	resourcesSuccessCount beam.Counter
	latencyMs             beam.Distribution
}

func (fnc *fhirioFnCommon) setup(namespace string) {
	if fnc.client == nil {
		fnc.client = newFhirStoreClient()
	}
	fnc.resourcesErrorCount = beam.NewCounter(namespace, baseMetricPrefix+"resource_error_count")
	fnc.resourcesSuccessCount = beam.NewCounter(namespace, baseMetricPrefix+"resource_success_count")
	fnc.latencyMs = beam.NewDistribution(namespace, baseMetricPrefix+"latency_ms")
}

type fhirStoreClient interface {
	readResource(resourcePath string) (*http.Response, error)
	executeBundle(storePath string, bundle []byte) (*http.Response, error)
}

type fhirStoreClientImpl struct {
	fhirService *healthcare.ProjectsLocationsDatasetsFhirStoresFhirService
}

func newFhirStoreClient() *fhirStoreClientImpl {
	healthcareService, err := healthcare.NewService(context.Background(), option.WithUserAgent(UserAgent))
	if err != nil {
		panic("Failed to initialize Google Cloud Healthcare Service. Reason: " + err.Error())
	}
	return &fhirStoreClientImpl{fhirService: healthcare.NewProjectsLocationsDatasetsFhirStoresFhirService(healthcareService)}
}

func (c *fhirStoreClientImpl) readResource(resourcePath string) (*http.Response, error) {
	return c.fhirService.Read(resourcePath).Do()
}

func (c *fhirStoreClientImpl) executeBundle(storePath string, bundle []byte) (*http.Response, error) {
	return c.fhirService.ExecuteBundle(storePath, bytes.NewReader(bundle)).Do()
}
