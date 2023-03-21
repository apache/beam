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

// Package urn handles extracting urns from all the protos.
package urns

import (
	"testing"

	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

// Test_toUrn validates that generic urn extraction mechnanism works, which is used for
// all the urns present.
func Test_toUrn(t *testing.T) {
	want := "beam:transform:pardo:v1"
	if got := TransformParDo; got != want {
		t.Errorf("TransformParDo = %v, want %v", got, want)
	}
	// Validate that quickUrn gets the same thing
	if got := quickUrn(pipepb.StandardPTransforms_PAR_DO); got != want {
		t.Errorf("quickUrn(\"pipepb.StandardPTransforms_PAR_DO\") = %v, want %v", got, want)
	}
}
