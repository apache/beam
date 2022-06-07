// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2
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
)

func validateResourceErrorCounter(t *testing.T, pipelineResult beam.PipelineResult, expectedCount int) {
	counterResults := pipelineResult.Metrics().AllMetrics().Counters()
	if len(counterResults) != 1 {
		t.Fatalf("counterResults got length %v, expected %v", len(counterResults), 1)
	}
	counterResult := counterResults[0]

	expectedCounterName := "fhirio/resource_error_count"
	if counterResult.Name() != expectedCounterName {
		t.Fatalf("counterResult.Name() is '%v', expected '%v'", counterResult.Name(), expectedCounterName)
	}

	if counterResult.Result() != int64(expectedCount) {
		t.Fatalf("counterResult.Result() is %v, expected %v", counterResult.Result(), expectedCount)
	}
}
