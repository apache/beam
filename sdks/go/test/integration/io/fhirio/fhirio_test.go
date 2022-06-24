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
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/fhirio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
	"google.golang.org/api/healthcare/v1"
	"google.golang.org/api/option"
)

const (
	datasetPathFmt = "projects/%s/locations/%s/datasets/apache-beam-integration-testing"
	testDataDir    = "../../../../data/fhir_bundles/"
)

var (
	storeService           *healthcare.ProjectsLocationsDatasetsFhirStoresFhirService
	storeManagementService *healthcare.ProjectsLocationsDatasetsFhirStoresService
)

func checkFlags(t *testing.T) {
	gcpProjectIsNotSet := gcpopts.Project == nil || *gcpopts.Project == ""
	if gcpProjectIsNotSet {
		t.Skip("GCP project flag is not set.")
	}
	gcpRegionIsNotSet := gcpopts.Region == nil || *gcpopts.Region == ""
	if gcpRegionIsNotSet {
		t.Skip("GCP region flag is not set.")
	}
}

func setupFhirStoreWithData(t *testing.T) (string, []string, func()) {
	return setupFhirStore(t, true)
}

func setupEmptyFhirStore(t *testing.T) (string, func()) {
	storePath, _, teardownFunc := setupFhirStore(t, false)
	return storePath, teardownFunc
}

// Sets up a test fhir store by creating and populating data to it for testing
// purposes. It returns the name of the created store path, a slice of the
// resource paths to be used in tests, and a function to teardown what has been
// set up.
func setupFhirStore(t *testing.T, shouldPopulateStore bool) (string, []string, func()) {
	t.Helper()
	if storeService == nil || storeManagementService == nil {
		t.Fatal("Healthcare Services were not initialized")
	}

	healthcareDataset := fmt.Sprintf(datasetPathFmt, *gcpopts.Project, *gcpopts.Region)
	createdFhirStore, err := createStore(healthcareDataset)
	if err != nil {
		t.Fatalf("Test store failed to be created. Reason: %v", err.Error())
	}
	createdFhirStorePath := createdFhirStore.Name

	var resourcePaths []string
	if shouldPopulateStore {
		resourcePaths = populateStore(createdFhirStorePath)
		if len(resourcePaths) == 0 {
			t.Fatal("No data got populated to test")
		}
	}

	return createdFhirStorePath, resourcePaths, func() {
		_, _ = deleteStore(createdFhirStorePath)
	}
}

func createStore(dataset string) (*healthcare.FhirStore, error) {
	randInt, _ := rand.Int(rand.Reader, big.NewInt(32))
	testFhirStoreId := "FHIR_store_write_it_" + strconv.FormatInt(time.Now().UnixMilli(), 10) + "_" + randInt.String()
	fhirStore := &healthcare.FhirStore{
		DisableReferentialIntegrity: true,
		EnableUpdateCreate:          true,
		Version:                     "R4",
	}
	return storeManagementService.Create(dataset, fhirStore).FhirStoreId(testFhirStoreId).Do()
}

func deleteStore(storePath string) (*healthcare.Empty, error) {
	return storeManagementService.Delete(storePath).Do()
}

// Populates fhir store with data. Note that failure to populate some data is not
// detrimental to the tests, so it is fine to ignore.
func populateStore(storePath string) []string {
	resourcePaths := make([]string, 0)
	for _, bundle := range readPrettyBundles() {
		response, err := storeService.ExecuteBundle(storePath, bytes.NewReader(bundle)).Do()
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
	return resourcePaths
}

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

func TestFhirIO_Read(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	_, testResourcePaths, teardownFhirStore := setupFhirStoreWithData(t)
	defer teardownFhirStore()

	p, s, resourcePaths := ptest.CreateList(testResourcePaths)
	resources, failedReads := fhirio.Read(s, resourcePaths)
	passert.Empty(s, failedReads)
	passert.Count(s, resources, "", len(testResourcePaths))

	ptest.RunAndValidate(t, p)
}

func TestFhirIO_InvalidRead(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	fhirStorePath, _, teardownFhirStore := setupFhirStoreWithData(t)
	defer teardownFhirStore()

	invalidResourcePath := fhirStorePath + "/fhir/Patient/invalid"
	p, s, resourcePaths := ptest.CreateList([]string{invalidResourcePath})
	resources, failedReads := fhirio.Read(s, resourcePaths)
	passert.Count(s, failedReads, "", 1)
	passert.Empty(s, resources)
	passert.True(s, failedReads, func(errorMsg string) bool {
		return strings.Contains(errorMsg, strconv.Itoa(http.StatusNotFound))
	})

	ptest.RunAndValidate(t, p)
}

func TestFhirIO_ExecuteBundles(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	fhirStorePath, teardownFhirStore := setupEmptyFhirStore(t)
	defer teardownFhirStore()

	p, s, bundles := ptest.CreateList(readPrettyBundles())
	successBodies, failures := fhirio.ExecuteBundles(s, fhirStorePath, bundles)
	passert.Count(s, successBodies, "", 2)
	passert.Count(s, failures, "", 2)
	passert.True(s, failures, func(errorMsg string) bool {
		return strings.Contains(errorMsg, strconv.Itoa(http.StatusBadRequest))
	})
	ptest.RunAndValidate(t, p)
}

func TestMain(m *testing.M) {
	flag.Parse()
	beam.Init()

	healthcareService, err := healthcare.NewService(context.Background(), option.WithUserAgent(fhirio.UserAgent))
	if err == nil {
		storeService = healthcare.NewProjectsLocationsDatasetsFhirStoresFhirService(healthcareService)
		storeManagementService = healthcare.NewProjectsLocationsDatasetsFhirStoresService(healthcareService)
	}

	ptest.MainRet(m)
}
