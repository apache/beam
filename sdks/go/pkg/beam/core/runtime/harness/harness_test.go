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

package harness

import (
	"fmt"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/golang/protobuf/proto"
)

// validDescriptor describes a valid pipeline with a source and a sink, but doesn't do anything else.
func validDescriptor(t *testing.T) *fnpb.ProcessBundleDescriptor {
	t.Helper()
	port := &fnpb.RemoteGrpcPort{
		CoderId: "c1",
		ApiServiceDescriptor: &pipepb.ApiServiceDescriptor{
			Url: "hostname:port",
		},
	}
	portBytes, err := proto.Marshal(port)
	if err != nil {
		t.Fatalf("bad port: %v", err)
	}
	return &fnpb.ProcessBundleDescriptor{
		Id: "test",
		Transforms: map[string]*pipepb.PTransform{
			"source": &pipepb.PTransform{
				Spec: &pipepb.FunctionSpec{
					Urn:     "beam:runner:source:v1",
					Payload: portBytes,
				},
				Outputs: map[string]string{
					"o1": "p1",
				},
			},
			"sink": &pipepb.PTransform{
				Spec: &pipepb.FunctionSpec{
					Urn:     "beam:runner:sink:v1",
					Payload: portBytes,
				},
				Inputs: map[string]string{
					"i1": "p1",
				},
			},
		},
		Pcollections: map[string]*pipepb.PCollection{
			"p1": &pipepb.PCollection{
				CoderId: "c1",
			},
		},
		Coders: map[string]*pipepb.Coder{
			"c1": &pipepb.Coder{
				Spec: &pipepb.FunctionSpec{
					Urn: "beam:coder:windowed_value:v1",
				},
				ComponentCoderIds: []string{"c2", "c3"},
			},
			"c2": &pipepb.Coder{
				Spec: &pipepb.FunctionSpec{
					Urn: "beam:coder:varint:v1",
				},
			},
			"c3": &pipepb.Coder{
				Spec: &pipepb.FunctionSpec{
					Urn: "beam:coder:global_window:v1",
				},
			},
		},
	}

}

func invalidDescriptor(t *testing.T) *fnpb.ProcessBundleDescriptor {
	return &fnpb.ProcessBundleDescriptor{}
}

func TestControl_getOrCreatePlan(t *testing.T) {
	testBDID := bundleDescriptorID("test")
	testPlan, err := exec.UnmarshalPlan(validDescriptor(t))
	if err != nil {
		t.Fatal("bad testPlan")
	}
	tests := []struct {
		name               string
		lookupErr, planErr error
		lookupDesc         *fnpb.ProcessBundleDescriptor
		descriptors        map[bundleDescriptorID]*fnpb.ProcessBundleDescriptor
		plans              map[bundleDescriptorID][]*exec.Plan
	}{
		{
			name:       "OK",
			lookupDesc: validDescriptor(t),
		}, {
			name: "cachedDescriptor",
			descriptors: map[bundleDescriptorID]*fnpb.ProcessBundleDescriptor{
				testBDID: validDescriptor(t),
			},
		}, {
			name: "cachedPlan",
			plans: map[bundleDescriptorID][]*exec.Plan{
				testBDID: []*exec.Plan{testPlan},
			},
		}, {
			name:      "badLookup",
			lookupErr: fmt.Errorf("lookupError"),
		}, {
			name:       "badDescriptorPlan",
			lookupDesc: invalidDescriptor(t),
			planErr:    fmt.Errorf("invalid bundle desc"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := &control{
				lookupDesc: func(bdID bundleDescriptorID) (*fnpb.ProcessBundleDescriptor, error) {
					return test.lookupDesc, test.lookupErr
				},
				descriptors: make(map[bundleDescriptorID]*fnpb.ProcessBundleDescriptor),
				plans:       make(map[bundleDescriptorID][]*exec.Plan),
				active:      make(map[instructionID]*exec.Plan),
				failed:      make(map[instructionID]error),
			}
			if test.descriptors != nil {
				ctrl.descriptors = test.descriptors
			}
			if test.plans != nil {
				ctrl.plans = test.plans
			}
			if test.planErr == nil {
				test.planErr = test.lookupErr
			}

			plan, err := ctrl.getOrCreatePlan(testBDID)
			if err != nil {
				if plan != nil {
					t.Error("getOrCreatePlan returned a non-nil error and non-nil plan. Non-nil errors must have nil plans.")
				}
				if got, want := err.Error(), test.planErr.Error(); !strings.Contains(got, want) {
					t.Errorf("getOrCreatePlan errored: got %q, want to contain %q", got, want)
				}
			}

		})
	}

}
