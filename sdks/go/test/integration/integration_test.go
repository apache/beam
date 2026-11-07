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

package integration

import (
	"regexp"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestDataflowJobName(t *testing.T) {
	firstID := uuid.MustParse("00112233-4455-4677-8899-aabbccddeeff")
	secondID := uuid.MustParse("10112233-4455-4677-8899-aabbccddeeff")
	testName := "TestParDo_MultiOutput/Long Subtest_Name"

	first := dataflowJobName(testName, firstID)
	second := dataflowJobName(testName, secondID)
	if first == second {
		t.Fatal("job names with different retained UUID bits must be distinct")
	}
	if !strings.HasPrefix(first, "go-testpardo-") {
		t.Fatalf("job name %q does not preserve a readable test name", first)
	}
	if len(first) > 40 {
		t.Fatalf("job name %q exceeds the 40-character compatibility limit", first)
	}
	if !regexp.MustCompile(`^[a-z][-a-z0-9]*[a-z0-9]$`).MatchString(first) {
		t.Fatalf("job name %q is not valid for Dataflow", first)
	}
}
