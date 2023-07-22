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
	"net/http"
	"regexp"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

const (
	bundleResponseTypeBatch       = "batch-response"
	bundleResponseTypeTransaction = "transaction-response"
)

func init() {
	register.DoFn4x0[context.Context, string, func(string), func(string)]((*executeBundleFn)(nil))
	register.Emitter1[string]()
}

type executeBundleFn struct {
	fnCommonVariables
	successesCount beam.Counter
	// Path to FHIR store where bundle requests will be executed on.
	FhirStorePath string
}

func (fn executeBundleFn) String() string {
	return "executeBundleFn"
}

func (fn *executeBundleFn) Setup() {
	fn.fnCommonVariables.setup(fn.String())
	fn.successesCount = beam.NewCounter(fn.String(), baseMetricPrefix+"success_count")
}

func (fn *executeBundleFn) ProcessElement(ctx context.Context, inputBundleBody string, emitSuccess, emitFailure func(string)) {
	response, err := executeAndRecordLatency(ctx, &fn.latencyMs, func() (*http.Response, error) {
		return fn.client.executeBundle(fn.FhirStorePath, inputBundleBody)
	})
	if err != nil {
		fn.resourcesErrorCount.Inc(ctx, 1)
		emitFailure(errors.Wrap(err, "execute bundle request returned error").Error())
		return
	}

	body, err := extractBodyFrom(response)
	if err != nil {
		fn.resourcesErrorCount.Inc(ctx, 1)
		emitFailure(errors.Wrap(err, "could not extract body from execute bundles response").Error())
		return
	}

	fn.processResponseBody(ctx, body, emitSuccess, emitFailure)
}

func (fn *executeBundleFn) processResponseBody(ctx context.Context, body string, emitSuccess, emitFailure func(string)) {
	var bodyFields struct {
		Type    string `json:"type"`
		Entries []any  `json:"entry"`
	}

	err := json.NewDecoder(strings.NewReader(body)).Decode(&bodyFields)
	if err != nil {
		fn.resourcesErrorCount.Inc(ctx, 1)
		emitFailure(errors.Wrap(err, "could not parse body from execute bundle response").Error())
		return
	}

	if bodyFields.Entries == nil {
		return
	}

	// A BATCH bundle returns a success response even if entries have failures, as
	// entries are executed separately. However, TRANSACTION bundles should return
	// error response (in client.executeBundle call) if any entry fails. Therefore,
	// for BATCH bundles we need to parse the error and success counters.
	switch bodyFields.Type {
	case bundleResponseTypeTransaction:
		fn.resourcesSuccessCount.Inc(ctx, int64(len(bodyFields.Entries)))
		emitSuccess(body)
	case bundleResponseTypeBatch:
		for _, entry := range bodyFields.Entries {
			var entryFields struct {
				Response struct {
					Status string `json:"status"`
				} `json:"response"`
			}
			entryBytes, _ := json.Marshal(entry)
			_ = json.NewDecoder(bytes.NewReader(entryBytes)).Decode(&entryFields)
			if entryFields.Response.Status == "" {
				continue
			}

			if batchResponseStatusIsBad(entryFields.Response.Status) {
				fn.resourcesErrorCount.Inc(ctx, 1)
				emitFailure(errors.Errorf("execute bundles entry contains bad status: [%v]", entryFields.Response.Status).Error())
			} else {
				fn.resourcesSuccessCount.Inc(ctx, 1)
				emitSuccess(string(entryBytes))
			}
		}
	}

	fn.successesCount.Inc(ctx, 1)
}

func batchResponseStatusIsBad(status string) bool {
	// 2XXs are successes, otherwise failure.
	isMatch, err := regexp.MatchString("^2\\d{2}", status)
	if err != nil {
		return true
	}
	return !isMatch
}

// ExecuteBundles performs all the requests in the specified bundles on a given
// FHIR store. This transform takes a path to a FHIR store and a PCollection of
// bundles as JSON-encoded strings. It executes the requests defined on the
// bundles on the FHIR store located on the provided path. It outputs two
// PCollection<string>, the first containing the response bodies of the
// successfully performed requests and the second one error messages of the
// requests that failed to be executed.
// See: https://cloud.google.com/healthcare-api/docs/samples/healthcare-fhir-execute-bundle
func ExecuteBundles(s beam.Scope, fhirStorePath string, bundles beam.PCollection) (beam.PCollection, beam.PCollection) {
	s = s.Scope("fhirio.ExecuteBundles")
	return executeBundles(s, fhirStorePath, bundles, nil)
}

// This is useful as an entry point for testing because we can provide a fake FHIR store client.
func executeBundles(s beam.Scope, fhirStorePath string, bundles beam.PCollection, client fhirStoreClient) (beam.PCollection, beam.PCollection) {
	return beam.ParDo2(s, &executeBundleFn{fnCommonVariables: fnCommonVariables{client: client}, FhirStorePath: fhirStorePath}, bundles)
}
