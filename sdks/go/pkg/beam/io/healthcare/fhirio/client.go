package fhirio

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/healthcare"
	healthcareapi "google.golang.org/api/healthcare/v1"
	"net/http"
)

type fhirStoreClient interface {
	readResource(resourceName string) (*http.Response, error)
}

type fhirStoreClientImpl struct {
	fhirService *healthcareapi.ProjectsLocationsDatasetsFhirStoresFhirService
}

func newFhirStoreClient() *fhirStoreClientImpl {
	healthcareService := healthcare.NewGcpHealthcareService()
	return &fhirStoreClientImpl{fhirService: healthcareapi.NewProjectsLocationsDatasetsFhirStoresFhirService(healthcareService)}
}

func (c *fhirStoreClientImpl) readResource(resourceName string) (*http.Response, error) {
	return c.fhirService.Read(resourceName).Do()
}
