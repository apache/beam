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
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"google.golang.org/api/healthcare/v1"
)

func TestMain(m *testing.M) {
	// TODO(https://github.com/apache/beam/issues/27547): Make tests compatible with portable runners.
	// To work on this change, replace call with `ptest.Main(m)`
	ptest.MainWithDefault(m, "direct")
}

func TestDeidentify_Error(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	out := deidentify(s, "src", "dst", nil, requestReturnErrorFakeClient)
	passert.Empty(s, out)

	result := ptest.RunAndValidate(t, p)
	validateCounter(t, result, operationErrorCounterName, 1)
	validateCounter(t, result, operationSuccessCounterName, 0)
	validateCounter(t, result, errorCounterName, 0)
	validateCounter(t, result, successCounterName, 0)
}

func TestDeidentify_Success(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	out := deidentify(s, "src", "dst", nil, &fakeFhirStoreClient{
		fakeDeidentify: func(string, string, *healthcare.DeidentifyConfig) (operationResults, error) {
			return testOperationResult, nil
		},
	})
	passert.Count(s, out, "", 1)

	result := ptest.RunAndValidate(t, p)
	validateCounter(t, result, operationErrorCounterName, 0)
	validateCounter(t, result, operationSuccessCounterName, 1)
	validateCounter(t, result, errorCounterName, int(testOperationResult.Failures))
	validateCounter(t, result, successCounterName, int(testOperationResult.Successes))
}
