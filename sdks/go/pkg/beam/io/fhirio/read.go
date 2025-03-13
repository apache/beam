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
	"context"
	"net/http"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn4x0[context.Context, []byte, func(string), func(string)]((*readResourceFn)(nil))
	register.Emitter1[string]()
}

type readResourceFn struct {
	fnCommonVariables
}

func (fn readResourceFn) String() string {
	return "readResourceFn"
}

func (fn *readResourceFn) Setup() {
	fn.fnCommonVariables.setup(fn.String())
}

func (fn *readResourceFn) ProcessElement(ctx context.Context, resourcePath []byte, emitResource, emitDeadLetter func(string)) {
	response, err := executeAndRecordLatency(ctx, &fn.latencyMs, func() (*http.Response, error) {
		return fn.client.readResource(resourcePath)
	})
	if err != nil {
		fn.resourcesErrorCount.Inc(ctx, 1)
		emitDeadLetter(errors.Wrapf(err, "read resource request returned error on input: [%v]", resourcePath).Error())
		return
	}

	body, err := extractBodyFrom(response)
	if err != nil {
		fn.resourcesErrorCount.Inc(ctx, 1)
		emitDeadLetter(errors.Wrapf(err, "could not extract body from read resource [%v] response", resourcePath).Error())
		return
	}

	fn.resourcesSuccessCount.Inc(ctx, 1)
	emitResource(body)
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

// This is useful as an entry point for testing because we can provide a fake FHIR store client.
func read(s beam.Scope, resourcePaths beam.PCollection, client fhirStoreClient) (beam.PCollection, beam.PCollection) {
	return beam.ParDo2(s, &readResourceFn{fnCommonVariables: fnCommonVariables{client: client}}, resourcePaths)
}
