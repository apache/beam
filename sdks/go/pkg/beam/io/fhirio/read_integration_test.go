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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

const testFhirVersion = "R4"

var (
	testDataDir       = fmt.Sprintf("../../../../data/healthcare/%s/", testFhirVersion)
	gcpProject        = flag.String("project", "apache-beam-testing", "GCP Project")
	healthcareDataset = fmt.Sprintf("projects/%s/locations/us-central1/datasets/apache-beam-integration-testing", *gcpProject)
	testFhirStoreName = "FHIR_store_" + testFhirVersion + "_write_it_" + strconv.FormatInt(time.Now().UnixMilli(), 10) + "_" + strconv.Itoa(rand.Intn(32))
)

func readPrettyBundles() [][]byte {
	files, _ := os.ReadDir(testDataDir)
	bundles := make([][]byte, len(files))
	for i, file := range files {
		bundles[i], _ = os.ReadFile(testDataDir + file.Name())
	}
	return bundles
}

func extractResourcePathFrom(resourceLocationUrl string) (string, error) {
	// The resource location url is in the following format:
	// https://healthcare.googleapis.com/v1/projects/PROJECT_ID/locations/LOCATION/datasets/DATASET_ID/fhirStores/STORE_ID/fhir/RESOURCE_NAME/RESOURCE_ID/_history/HISTORY_ID
	// But the API calls use this format: projects/PROJECT_ID/locations/LOCATION/datasets/DATASET_ID/fhirStores/STORE_ID/fhir/RESOURCE_NAME/RESOURCE_ID
	startIdx := strings.Index(resourceLocationUrl, "projects/")
	endIdx := strings.Index(resourceLocationUrl, "/_history")
	if startIdx == -1 || endIdx == -1 {
		return "", errors.New("resource location url is invalid")
	}
	return resourceLocationUrl[startIdx:endIdx], nil
}

// Sets up a test fhir store by creating and populating data to it for testing
// purposes. It returns the name of the created store path, a slice of the
// resource paths to be used in tests, and a function to teardown what has been
// set up. Note: failure to populate some data is not detrimental to the tests,
// so it is fine to ignore.
func setupSuite(t *testing.T) (string, []string, func()) {
	client := newFhirStoreClient()
	createdFhirStore, err := client.createStore(healthcareDataset, testFhirStoreName, testFhirVersion)
	if err != nil {
		t.Fatal("Test store failed to be created")
	}
	resourcePaths := make([]string, 0)
	for _, bundle := range readPrettyBundles() {
		response, err := client.executeBundle(healthcareDataset, testFhirStoreName, bundle)
		if err != nil {
			continue
		}

		var body struct {
			Entry []struct {
				Response struct {
					Location string `json:"location"`
					Status   string `json:"status"`
				} `json:"response"`
			} `json:"entry"`
		}
		err = json.NewDecoder(response.Body).Decode(&body)
		if err != nil {
			continue
		}

		for _, entry := range body.Entry {
			bundleFailedToBeCreated := !strings.Contains(entry.Response.Status, "201")
			if bundleFailedToBeCreated {
				continue
			}

			resourcePath, err := extractResourcePathFrom(entry.Response.Location)
			if err != nil {
				continue
			}
			resourcePaths = append(resourcePaths, resourcePath)
		}
	}

	if len(resourcePaths) == 0 {
		t.Fatal("No data got populated to test")
	}

	return createdFhirStore.Name, resourcePaths, func() {
		_, _ = client.deleteStore(healthcareDataset, testFhirStoreName)
	}
}

func TestRead_Success(t *testing.T) {
	_, testResourcePaths, teardownSuite := setupSuite(t)
	defer teardownSuite()

	p, s, resourcePaths := ptest.CreateList(testResourcePaths)
	resources, failedReads := Read(s, resourcePaths)
	passert.Empty(s, failedReads)
	passert.Count(s, resources, "", len(testResourcePaths))
	pipelineResult := ptest.RunAndValidate(t, p)
	validateCounters(t, pipelineResult.Metrics().AllMetrics().Counters(), []struct {
		string
		int64
	}{
		{"fhirio/read_resource_success_count", int64(len(testResourcePaths))},
	})
}

func TestRead_InvalidResourcePath(t *testing.T) {
	fhirStorePath, _, teardownSuite := setupSuite(t)
	defer teardownSuite()

	invalidResourcePath := fhirStorePath + "/fhir/Patient/invalid"
	p, s, resourcePaths := ptest.CreateList([]string{invalidResourcePath})
	resources, failedReads := Read(s, resourcePaths)
	passert.Count(s, failedReads, "", 1)
	passert.Empty(s, resources)
	passert.True(s, failedReads, func(errorMsg string) bool {
		return strings.Contains(errorMsg, "bad status [404]")
	})
	pipelineResult := ptest.RunAndValidate(t, p)
	validateCounters(t, pipelineResult.Metrics().AllMetrics().Counters(), []struct {
		string
		int64
	}{
		{"fhirio/read_resource_error_count", 1},
	})
}
