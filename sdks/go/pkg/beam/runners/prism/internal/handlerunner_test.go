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

package internal

import (
	"testing"

	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestHandleReshuffle(t *testing.T) {
	h := &runner{
		config: RunnerCharacteristic{
			SDKReshuffle: false,
		},
	}

	comps := &pipepb.Components{
		Transforms: map[string]*pipepb.PTransform{
			"reshuffle": {
				UniqueName: "reshuffle",
				Inputs: map[string]string{
					"in": "pcol_in",
				},
				Outputs: map[string]string{
					"out": "pcol_out",
				},
				Subtransforms: []string{"sub_1"},
			},
			"sub_1": {
				UniqueName:    "sub_1",
				Subtransforms: []string{"sub_2"},
			},
			"sub_2": {
				UniqueName: "sub_2",
			},
			"consumer": {
				UniqueName: "consumer",
				Inputs: map[string]string{
					"in": "pcol_out",
				},
			},
		},
	}

	got := h.handleReshuffle("reshuffle", comps.GetTransforms()["reshuffle"], comps)

	want := prepareResult{
		SubbedComps:   nil,
		RemovedLeaves: []string{"sub_1", "sub_2"},
		ForcedRoots:   []string{"consumer"},
	}

	if d := cmp.Diff(want, got, protocmp.Transform()); d != "" {
		t.Errorf("handleReshuffle() diff (-want, +got):\n%v", d)
	}

	// Verify that the consumer's input has been remapped to the input of the reshuffle
	gotInput := comps.GetTransforms()["consumer"].GetInputs()["in"]
	if gotInput != "pcol_in" {
		t.Errorf("consumer input got %q, want %q", gotInput, "pcol_in")
	}
}
