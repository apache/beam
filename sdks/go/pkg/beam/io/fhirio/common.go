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
package fhirio

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/healthcare/v1"
	"google.golang.org/api/option"
)

const (
	UserAgent                   = "apache-beam-io-google-cloud-platform-healthcare/" + core.SdkVersion
	baseMetricPrefix            = "fhirio/"
	errorCounterName            = baseMetricPrefix + "resource_error_count"
	operationErrorCounterName   = baseMetricPrefix + "operation_error_count"
	operationSuccessCounterName = baseMetricPrefix + "operation_success_count"
	successCounterName          = baseMetricPrefix + "resource_success_count"
	pageTokenParameterKey       = "_page_token"
)

var backoffDuration = [...]time.Duration{time.Second, 5 * time.Second, 10 * time.Second, 15 * time.Second}

func executeAndRecordLatency[T any](ctx context.Context, latencyMs *beam.Distribution, executionSupplier func() (T, error)) (T, error) {
	timeBeforeReadRequest := time.Now()
	result, err := executionSupplier()
	latencyMs.Update(ctx, time.Since(timeBeforeReadRequest).Milliseconds())
	return result, err
}

func extractBodyFrom(response *http.Response) (string, error) {
	err := googleapi.CheckResponse(response)
	if err != nil {
		return "", errors.Wrapf(err, "response contains bad status: [%v]", response.Status)
	}

	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}

	return string(bodyBytes), nil
}

type operationCounters struct {
	successCount, errorCount beam.Counter
}

func (c *operationCounters) setup(namespace string) {
	c.successCount = beam.NewCounter(namespace, operationSuccessCounterName)
	c.errorCount = beam.NewCounter(namespace, operationErrorCounterName)
}

type operationResults struct {
	Successes int64 `json:"success,string"`
	Failures  int64 `json:"failure,string"`
}

type fhirStoreClient interface {
	readResource(resourcePath []byte) (*http.Response, error)
	executeBundle(storePath string, bundle string) (*http.Response, error)
	search(storePath, resourceType string, queries map[string]string, pageToken string) (*http.Response, error)
	deidentify(srcStorePath, dstStorePath string, deidConfig *healthcare.DeidentifyConfig) (operationResults, error)
	importResources(storePath, gcsURI string, contentStructure ContentStructure) (operationResults, error)
}

type fhirStoreClientImpl struct {
	healthcareService *healthcare.Service
}

func newFhirStoreClient() *fhirStoreClientImpl {
	healthcareService, err := healthcare.NewService(context.Background(), option.WithUserAgent(UserAgent))
	if err != nil {
		panic("Failed to initialize Google Cloud Healthcare Service. Reason: " + err.Error())
	}
	return &fhirStoreClientImpl{healthcareService}
}

func (c *fhirStoreClientImpl) fhirService() *healthcare.ProjectsLocationsDatasetsFhirStoresFhirService {
	return c.healthcareService.Projects.Locations.Datasets.FhirStores.Fhir
}

func (c *fhirStoreClientImpl) fhirStoreService() *healthcare.ProjectsLocationsDatasetsFhirStoresService {
	return c.healthcareService.Projects.Locations.Datasets.FhirStores
}

func (c *fhirStoreClientImpl) readResource(resourcePath []byte) (*http.Response, error) {
	return c.fhirService().Read(string(resourcePath)).Do()
}

func (c *fhirStoreClientImpl) executeBundle(storePath, bundle string) (*http.Response, error) {
	return c.fhirService().ExecuteBundle(storePath, strings.NewReader(bundle)).Do()
}

func (c *fhirStoreClientImpl) search(storePath, resourceType string, queries map[string]string, pageToken string) (*http.Response, error) {
	queryParams := make([]googleapi.CallOption, 0)
	for key, value := range queries {
		queryParams = append(queryParams, googleapi.QueryParameter(key, value))
	}

	if pageToken != "" {
		queryParams = append(queryParams, googleapi.QueryParameter(pageTokenParameterKey, pageToken))
	}

	searchRequest := &healthcare.SearchResourcesRequest{}
	if resourceType == "" {
		return c.fhirService().Search(storePath, searchRequest).Do(queryParams...)
	}
	return c.fhirService().SearchType(storePath, resourceType, searchRequest).Do(queryParams...)
}

func (c *fhirStoreClientImpl) deidentify(srcStorePath, dstStorePath string, deidConfig *healthcare.DeidentifyConfig) (operationResults, error) {
	deidRequest := &healthcare.DeidentifyFhirStoreRequest{
		Config:           deidConfig,
		DestinationStore: dstStorePath,
	}
	operation, err := c.fhirStoreService().Deidentify(srcStorePath, deidRequest).Do()
	if err != nil {
		return operationResults{}, err
	}
	return c.pollTilCompleteAndCollectResults(operation)
}

func (c *fhirStoreClientImpl) importResources(storePath, gcsURI string, contentStructure ContentStructure) (operationResults, error) {
	importRequest := &healthcare.ImportResourcesRequest{
		ContentStructure: contentStructure.String(),
		GcsSource:        &healthcare.GoogleCloudHealthcareV1FhirGcsSource{Uri: gcsURI},
	}
	operation, err := c.fhirStoreService().Import(storePath, importRequest).Do()
	if err != nil {
		return operationResults{}, err
	}
	return c.pollTilCompleteAndCollectResults(operation)
}

func (c *fhirStoreClientImpl) pollTilCompleteAndCollectResults(operation *healthcare.Operation) (operationResults, error) {
	operation, err := c.healthcareService.Projects.Locations.Datasets.Operations.Get(operation.Name).Do()
	for i := 0; err == nil && !operation.Done; {
		time.Sleep(backoffDuration[i])
		if i < len(backoffDuration)-1 {
			i += 1
		}

		operation, err = c.healthcareService.Projects.Locations.Datasets.Operations.Get(operation.Name).Do()
	}

	if err != nil {
		return operationResults{}, err
	}

	if operation.Error != nil {
		return operationResults{}, errors.New(operation.Error.Message)
	}

	return parseOperationCounterResultsFrom(operation.Metadata)
}

func parseOperationCounterResultsFrom(operationMetadata []byte) (operationResults, error) {
	var operationCounterField struct {
		Counter struct {
			operationResults
		} `json:"counter"`
	}
	err := json.NewDecoder(bytes.NewReader(operationMetadata)).Decode(&operationCounterField)
	if err != nil {
		return operationResults{}, err
	}
	return operationCounterField.Counter.operationResults, nil
}

type fnCommonVariables struct {
	client                fhirStoreClient
	resourcesErrorCount   beam.Counter
	resourcesSuccessCount beam.Counter
	latencyMs             beam.Distribution
}

func (fnc *fnCommonVariables) setup(namespace string) {
	if fnc.client == nil {
		fnc.client = newFhirStoreClient()
	}
	fnc.resourcesErrorCount = beam.NewCounter(namespace, errorCounterName)
	fnc.resourcesSuccessCount = beam.NewCounter(namespace, successCounterName)
	fnc.latencyMs = beam.NewDistribution(namespace, baseMetricPrefix+"latency_ms")
}
