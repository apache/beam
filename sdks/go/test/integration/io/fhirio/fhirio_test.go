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
	datasetPathFmt  = "projects/%s/locations/%s/datasets/apache-beam-integration-testing"
	tempStoragePath = "gs://temp-storage-for-end-to-end-tests"
	testDataDir     = "../../../../data/fhir_bundles/"
)

var (
	storeService           *healthcare.ProjectsLocationsDatasetsFhirStoresFhirService
	storeManagementService *healthcare.ProjectsLocationsDatasetsFhirStoresService
)

type fhirStoreInfo struct {
	path           string
	resourcesPaths [][]byte
}

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

func setupFhirStoreWithData(t *testing.T) (fhirStoreInfo, func()) {
	return setupFhirStore(t, true)
}

func setupEmptyFhirStore(t *testing.T) (string, func()) {
	storeInfo, teardown := setupFhirStore(t, false)
	return storeInfo.path, teardown
}

// Sets up a test fhir store by creating and populating data to it for testing
// purposes. It returns the name of the created store path, a slice of the
// resource paths to be used in tests, and a function to teardown what has been
// set up.
func setupFhirStore(t *testing.T, shouldPopulateStore bool) (fhirStoreInfo, func()) {
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

	var resourcePaths [][]byte
	if shouldPopulateStore {
		resourcePaths = populateStore(createdFhirStorePath)
		if len(resourcePaths) == 0 {
			t.Fatal("No data got populated to test")
		}
	}

	return fhirStoreInfo{
			path:           createdFhirStorePath,
			resourcesPaths: resourcePaths,
		}, func() {
			_, _ = deleteStore(createdFhirStorePath)
		}
}

func createStore(dataset string) (*healthcare.FhirStore, error) {
	randInt, _ := rand.Int(rand.Reader, big.NewInt(32))
	testFhirStoreID := "FHIR_store_write_it_" + strconv.FormatInt(time.Now().UnixMilli(), 10) + "_" + randInt.String()
	fhirStore := &healthcare.FhirStore{
		DisableReferentialIntegrity: true,
		EnableUpdateCreate:          true,
		Version:                     "R4",
	}
	return storeManagementService.Create(dataset, fhirStore).FhirStoreId(testFhirStoreID).Do()
}

func deleteStore(storePath string) (*healthcare.Empty, error) {
	return storeManagementService.Delete(storePath).Do()
}

// Populates fhir store with data. Note that failure to populate some data is not
// detrimental to the tests, so it is fine to ignore.
func populateStore(storePath string) [][]byte {
	resourcePaths := make([][]byte, 0)
	for _, bundle := range readPrettyBundles() {
		response, err := storeService.ExecuteBundle(storePath, strings.NewReader(bundle)).Do()
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

func readPrettyBundles() []string {
	files, _ := os.ReadDir(testDataDir)
	bundles := make([]string, len(files))
	for i, file := range files {
		bundle, _ := os.ReadFile(testDataDir + file.Name())
		bundles[i] = string(bundle)
	}
	return bundles
}

func extractResourcePathFrom(resourceLocationURL string) ([]byte, error) {
	// The resource location url is in the following format:
	// https://healthcare.googleapis.com/v1/projects/PROJECT_ID/locations/LOCATION/datasets/DATASET_ID/fhirStores/STORE_ID/fhir/RESOURCE_NAME/RESOURCE_ID/_history/HISTORY_ID
	// But the API calls use this format: projects/PROJECT_ID/locations/LOCATION/datasets/DATASET_ID/fhirStores/STORE_ID/fhir/RESOURCE_NAME/RESOURCE_ID
	startIdx := strings.Index(resourceLocationURL, "projects/")
	endIdx := strings.Index(resourceLocationURL, "/_history")
	if startIdx == -1 || endIdx == -1 {
		return nil, errors.New("resource location url is invalid")
	}
	return []byte(resourceLocationURL[startIdx:endIdx]), nil
}

func readTestTask(t *testing.T, s beam.Scope, testStoreInfo fhirStoreInfo) func() {
	t.Helper()

	s = s.Scope("fhirio_test.readTestTask")
	testResources := append(testStoreInfo.resourcesPaths, []byte(testStoreInfo.path+"/fhir/Patient/invalid"))
	resourcePathsPCollection := beam.CreateList(s, testResources)
	resources, failedReads := fhirio.Read(s, resourcePathsPCollection)
	passert.Count(s, resources, "", len(testStoreInfo.resourcesPaths))
	passert.Count(s, failedReads, "", 1)
	return nil
}

func executeBundlesTestTask(t *testing.T, s beam.Scope, _ fhirStoreInfo) func() {
	t.Helper()

	s = s.Scope("fhirio_test.executeBundlesTestTask")
	fhirStorePath, teardownFhirStore := setupEmptyFhirStore(t)
	bundlesPCollection := beam.CreateList(s, readPrettyBundles())
	successBodies, failures := fhirio.ExecuteBundles(s, fhirStorePath, bundlesPCollection)
	passert.Count(s, successBodies, "", 2)
	passert.Count(s, failures, "", 2)
	passert.True(s, failures, func(errorMsg string) bool {
		return strings.Contains(errorMsg, strconv.Itoa(http.StatusBadRequest))
	})
	return teardownFhirStore
}

func searchTestTask(t *testing.T, s beam.Scope, testStoreInfo fhirStoreInfo) func() {
	t.Helper()

	s = s.Scope("fhirio_test.searchTestTask")
	searchQueries := []fhirio.SearchQuery{
		{},
		{ResourceType: "Patient"},
		{ResourceType: "Patient", Parameters: map[string]string{"gender": "female", "family:contains": "Smith"}},
		{ResourceType: "Encounter"},
	}
	searchQueriesCol := beam.CreateList(s, searchQueries)
	searchResult, deadLetter := fhirio.Search(s, testStoreInfo.path, searchQueriesCol)
	passert.Empty(s, deadLetter)
	passert.Count(s, searchResult, "", len(searchQueries))

	resourcesFoundCount := beam.ParDo(s, func(identifier string, resourcesFound []string) int {
		return len(resourcesFound)
	}, searchResult)
	passert.Equals(s, resourcesFoundCount, 4, 2, 1, 0)
	return nil
}

func deidentifyTestTask(t *testing.T, s beam.Scope, testStoreInfo fhirStoreInfo) func() {
	t.Helper()

	s = s.Scope("fhirio_test.deidentifyTestTask")
	dstFhirStorePath, teardownDstFhirStore := setupEmptyFhirStore(t)
	res := fhirio.Deidentify(s, testStoreInfo.path, dstFhirStorePath, &healthcare.DeidentifyConfig{})
	passert.Count(s, res, "", 1)
	return teardownDstFhirStore
}

func importTestTask(t *testing.T, s beam.Scope, _ fhirStoreInfo) func() {
	t.Helper()

	s = s.Scope("fhirio_test.importTestTask")

	fhirStorePath, teardownFhirStore := setupEmptyFhirStore(t)

	patientTestResource := `{"resourceType":"Patient","id":"c1q34623-b02c-3f8b-92ea-873fc4db60da","name":[{"use":"official","family":"Smith","given":["Alice"]}],"gender":"female","birthDate":"1970-01-01"}`
	practitionerTestResource := `{"resourceType":"Practitioner","id":"b0e04623-b02c-3f8b-92ea-943fc4db60da","name":[{"family":"Tillman293","given":["Franklin857"],"prefix":["Dr."]}],"address":[{"line":["295 VARNUM AVENUE"],"city":"LOWELL","state":"MA","postalCode":"01854","country":"US"}],"gender":"male"}`
	testResources := beam.Create(s, patientTestResource, practitionerTestResource)

	failedResources, deadLetter := fhirio.Import(s, fhirStorePath, tempStoragePath, tempStoragePath, fhirio.ContentStructureResource, testResources)
	passert.Empty(s, failedResources)
	passert.Empty(s, deadLetter)

	return teardownFhirStore
}

func TestFhirIO(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	testStoreInfo, teardownFhirStore := setupFhirStoreWithData(t)
	defer teardownFhirStore()

	p, s := beam.NewPipelineWithRoot()

	type testTask func(*testing.T, beam.Scope, fhirStoreInfo) func()
	testTasks := []testTask{
		readTestTask,
		executeBundlesTestTask,
		searchTestTask,
		deidentifyTestTask,
		importTestTask,
	}
	teardownTasks := make([]func(), len(testTasks))
	for i, testTaskCallable := range testTasks {
		teardownTasks[i] = testTaskCallable(t, s, testStoreInfo)
	}

	defer func() {
		for _, teardown := range teardownTasks {
			if teardown != nil {
				teardown()
			}
		}
	}()

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
