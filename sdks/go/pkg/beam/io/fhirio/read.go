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
	"io"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn4x0[context.Context, string, func(string), func(string)]((*readResourceFn)(nil))
	register.Emitter1[string]()
}

type readResourceFn struct {
	client                fhirStoreClient
	readResourceErrors    beam.Counter
	readResourceSuccess   beam.Counter
	readResourceLatencyMs beam.Distribution
}

func (fn readResourceFn) String() string {
	return "readResourceFn"
}

func (fn *readResourceFn) Setup() {
	if fn.client == nil {
		fn.client = newFhirStoreClient()
	}
	fn.readResourceErrors = beam.NewCounter(fn.String(), baseMetricPrefix+"read_resource_error_count")
	fn.readResourceSuccess = beam.NewCounter(fn.String(), baseMetricPrefix+"read_resource_success_count")
	fn.readResourceLatencyMs = beam.NewDistribution(fn.String(), baseMetricPrefix+"read_resource_latency_ms")
}

func (fn *readResourceFn) ProcessElement(ctx context.Context, resourcePath string, emitResource, emitDeadLetter func(string)) {
	timeBeforeReadRequest := time.Now()
	response, err := fn.client.readResource(resourcePath)
	fn.readResourceLatencyMs.Update(ctx, time.Since(timeBeforeReadRequest).Milliseconds())

	if err != nil {
		fn.readResourceErrors.Inc(ctx, 1)
		emitDeadLetter(errors.Wrapf(err, "failed fetching resource [%s]", resourcePath).Error())
		return
	}

	if response.StatusCode != 200 {
		fn.readResourceErrors.Inc(ctx, 1)
		emitDeadLetter(errors.Errorf("fetched resource [%s] returned bad status [%d]", resourcePath, response.StatusCode).Error())
		return
	}

	bytes, err := io.ReadAll(response.Body)
	if err != nil {
		fn.readResourceErrors.Inc(ctx, 1)
		emitDeadLetter(errors.Wrapf(err, "error reading response body of resource [%s]", resourcePath).Error())
		return
	}

	fn.readResourceSuccess.Inc(ctx, 1)
	emitResource(string(bytes))
}

// Read fetches resources from Google Cloud Healthcare FHIR stores based on the
// resource path. It consumes a PCollection<string> of notifications from the
// FHIR store of resource paths, and fetches the actual resource object on the
// path in the notification. It outputs two PCollection<string>. The first
// contains the fetched object as a JSON-encoded string, and the second is a
// dead-letter with an error message, in case the object failed to be fetched.
// See: https://cloud.google.com/healthcare-api/docs/how-tos/fhir-resources#getting_a_fhir_resource.
func Read(s beam.Scope, resourcePaths beam.PCollection) (beam.PCollection, beam.PCollection) {
	s = s.Scope("fhirio.Read")
	return read(s, resourcePaths, nil)
}

func read(s beam.Scope, resourcePaths beam.PCollection, client fhirStoreClient) (beam.PCollection, beam.PCollection) {
	return beam.ParDo2(s, &readResourceFn{client: client}, resourcePaths)
}
