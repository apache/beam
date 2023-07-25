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

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"google.golang.org/api/healthcare/v1"
)

func init() {
	register.DoFn3x0[context.Context, []byte, func(string)]((*deidentifyFn)(nil))
	register.Emitter1[string]()
}

type deidentifyFn struct {
	fnCommonVariables
	operationCounters
	SourceStorePath, DestinationStorePath string
	DeidentifyConfig                      *healthcare.DeidentifyConfig
}

func (fn deidentifyFn) String() string {
	return "deidentifyFn"
}

func (fn *deidentifyFn) Setup() {
	fn.fnCommonVariables.setup(fn.String())
	fn.operationCounters.setup(fn.String())
}

func (fn *deidentifyFn) ProcessElement(ctx context.Context, _ []byte, emitDstStore func(string)) {
	result, err := executeAndRecordLatency(ctx, &fn.latencyMs, func() (operationResults, error) {
		return fn.client.deidentify(fn.SourceStorePath, fn.DestinationStorePath, fn.DeidentifyConfig)
	})
	if err != nil {
		log.Warnf(ctx, "Deidentify operation failed. Reason: %v", err)
		fn.operationCounters.errorCount.Inc(ctx, 1)
		return
	}

	fn.operationCounters.successCount.Inc(ctx, 1)
	fn.resourcesSuccessCount.Inc(ctx, result.Successes)
	fn.resourcesErrorCount.Inc(ctx, result.Failures)
	emitDstStore(fn.DestinationStorePath)
}

// Deidentify transform de-identifies sensitive data in resources located in a
// Google Cloud FHIR store. It receives a source and destination store paths as
// well as de-identification configuration (
// https://cloud.google.com/healthcare-api/docs/reference/rest/v1/DeidentifyConfig#FhirConfig).
// It performs de-identification on the source store using the provided
// configuration and applies the result in the destination store. It outputs a
// PCollection containing the destination store path if de-identification was
// performed successfully, otherwise it returns an empty PCollection.
// See: https://cloud.google.com/healthcare-api/docs/how-tos/fhir-deidentify
func Deidentify(s beam.Scope, srcStore, dstStore string, config *healthcare.DeidentifyConfig) beam.PCollection {
	s = s.Scope("fhirio.Deidentify")
	return deidentify(s, srcStore, dstStore, config, nil)
}

func deidentify(s beam.Scope, srcStore, dstStore string, config *healthcare.DeidentifyConfig, client fhirStoreClient) beam.PCollection {
	imp := beam.Impulse(s)
	return beam.ParDo(
		s,
		&deidentifyFn{
			fnCommonVariables:    fnCommonVariables{client: client},
			SourceStorePath:      srcStore,
			DestinationStorePath: dstStore,
			DeidentifyConfig:     config,
		},
		imp,
	)
}
