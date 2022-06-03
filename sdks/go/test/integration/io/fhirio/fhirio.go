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

// Package fhirio contains integration tests for FhirIO transforms.
package fhirio

import (
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/fhirio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func ReadPipeline(testResourcePaths []string) *beam.Pipeline {
	p, s, resourcePaths := ptest.CreateList(testResourcePaths)
	resources, failedReads := fhirio.Read(s, resourcePaths)
	passert.Empty(s, failedReads)
	passert.Count(s, resources, "", len(testResourcePaths))
	return p
}

func InvalidReadPipeline(fhirStorePath string) *beam.Pipeline {
	invalidResourcePath := fhirStorePath + "/fhir/Patient/invalid"
	p, s, resourcePaths := ptest.CreateList([]string{invalidResourcePath})
	resources, failedReads := fhirio.Read(s, resourcePaths)
	passert.Count(s, failedReads, "", 1)
	passert.Empty(s, resources)
	passert.True(s, failedReads, func(errorMsg string) bool {
		return strings.Contains(errorMsg, "bad status [404]")
	})
	return p
}
