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
	"context"
	"net/http"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core"
	"google.golang.org/api/healthcare/v1"
	"google.golang.org/api/option"
)

const (
	UserAgent        = "apache-beam-io-google-cloud-platform-healthcare/" + core.SdkVersion
	baseMetricPrefix = "fhirio/"
)

type fhirStoreClient interface {
	readResource(resourcePath string) (*http.Response, error)
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
