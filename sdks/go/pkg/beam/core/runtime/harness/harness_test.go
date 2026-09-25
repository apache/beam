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
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/protobuf/proto"
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
	testPlan, err := exec.UnmarshalPlan(validDescriptor(t), nil)
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

func TestFail(t *testing.T) {
	ctx := context.Background()
	resp := fail(ctx, "test-id", "error %s %d", "msg", 42)

	if resp == nil {
		t.Fatal("fail returned nil")
	}
	if got, want := resp.GetInstructionId(), "test-id"; got != want {
		t.Errorf("got InstructionId %v, want %v", got, want)
	}
	if !strings.Contains(resp.GetError(), "error msg 42") {
		t.Errorf("got Error %v, want to contain 'error msg 42'", resp.GetError())
	}
	if resp.GetRegister() == nil {
		t.Error("expected Register dummy response to be non-nil")
	}
}

func TestCircleBuffer(t *testing.T) {
	expected1 := instructionID("expected1")
	expected2 := instructionID("expected2")

	t.Run("Contains", func(t *testing.T) {
		c := newCircleBuffer()
		c.Add(expected1)
		if !c.Contains(expected1) {
			t.Fatal("expected added key to be present")
		}
		rm, ok := c.Insert(expected2)
		if ok {
			t.Fatalf("unexpected eviction of %v on Insert", rm)
		}
		if !c.Contains(expected2) {
			t.Fatal("expected added key to be present")
		}
	})

	t.Run("Remove", func(t *testing.T) {
		c := newCircleBuffer()
		c.Add(expected1)
		c.Remove(expected1)
		if c.Contains(expected1) {
			t.Fatal("unexpected removed key present")
		}
		rm, ok := c.Insert(expected2)
		if ok {
			t.Fatalf("unexpected eviction of %v on Insert", rm)
		}
		c.Remove(expected2)
		if c.Contains(expected2) {
			t.Fatal("unexpected removed key present")
		}
	})

	t.Run("Insert", func(t *testing.T) {
		c := newCircleBuffer()
		rm, ok := c.Insert(expected1)
		if ok {
			t.Fatalf("unexpected eviction of %v on Insert", rm)
		}
		for i := 1; i < circleBufferCap; i++ {
			rm, ok := c.Insert(instructionID(fmt.Sprintf("i%v", i)))
			if ok {
				t.Fatalf("unexpected eviction of %v on Insert", rm)
			}
		}
		got, ok := c.Insert(expected2)
		if !ok {
			t.Fatalf("expected eviction of %v on Insert", expected1)
		}
		if got != expected1 {
			t.Fatalf("c.Insert(%v) = %v,%v; want %v", expected2, got, ok, expected1)
		}
		for i := 1; i < circleBufferCap; i++ {
			insrt := instructionID(fmt.Sprintf("ii%v", i))
			got, ok := c.Insert(insrt)
			want := instructionID(fmt.Sprintf("i%v", i))
			if got != want {
				t.Fatalf("c.Insert(%v) = %v,%v; want %v", insrt, got, ok, want)
			}
		}
	})
}

func TestElementProcessingTimeoutParsing(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		in   string
		want time.Duration
		err  bool
	}{
		{"5m", 5 * time.Minute, false},
		{"1h", 1 * time.Hour, false},
		{"1m5s", 1*time.Minute + 5*time.Second, false},
		{"5s1m", 5*time.Second + 1*time.Minute, false},
		{"-1", 0, true},
		{"", 0, true},
		{"5mmm", 0, true},
	}

	for _, test := range tests {
		got, err := parseTimeoutDurationFlag(ctx, test.in)
		if (err != nil) != test.err {
			t.Errorf("parseTimeoutDurationFlag(ctx, %q) err = %v, want err? %v", test.in, err, test.err)
		}
		if got != test.want {
			t.Errorf("parseTimeoutDurationFlag(ctx, %q) = %v, want %v", test.in, got, test.want)
		}
	}
}

func TestControl_MetStoreToString(t *testing.T) {
	ctx := metrics.SetBundleID(context.Background(), "test-bundle")
	store := metrics.GetStore(ctx)
	if store == nil {
		t.Fatal("GetStore returned nil")
	}
	ctrl := &control{
		metStore: map[instructionID]*metrics.Store{
			"inst1": store,
		},
	}
	b := &strings.Builder{}
	ctrl.metStoreToString(b)
	out := b.String()
	if !strings.Contains(out, "Bundle ID: inst1") {
		t.Errorf("metStoreToString output missing bundle ID, got: %s", out)
	}
}

func TestControl_GetPlanOrResponse(t *testing.T) {
	tests := []struct {
		name           string
		active         map[instructionID]*exec.Plan
		awaitFinalize  map[instructionID]awaitingFinalization
		failed         map[instructionID]error
		inactive       circleBuffer
		wantErr        bool // response has Error field set (non-nil response)
		wantNilPlan    bool // response is non-nil and plan is nil -> response is returned
		wantEmpty      bool // both plan and response are nil - empty response needed
	}{
		{
			name: "active",
			active: map[instructionID]*exec.Plan{
				"ref": {},
			},
		},
		{
			name: "awaitingFinalization",
			awaitFinalize: map[instructionID]awaitingFinalization{
				"ref": {plan: &exec.Plan{}},
			},
		},
		{
			name:    "failed",
			failed:  map[instructionID]error{"ref": fmt.Errorf("test failure")},
			wantErr: true,
		},
		{
			name: "inactive",
			inactive: func() circleBuffer {
				c := newCircleBuffer()
				c.Add("ref")
				return c
			}(),
			wantEmpty: true,
		},
		{
			name:       "notFound",
			wantErr:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := &control{
				active:               make(map[instructionID]*exec.Plan),
				awaitingFinalization: make(map[instructionID]awaitingFinalization),
				failed:               make(map[instructionID]error),
				inactive:             newCircleBuffer(),
			}
			if test.active != nil {
				ctrl.active = test.active
			}
			if test.awaitFinalize != nil {
				ctrl.awaitingFinalization = test.awaitFinalize
			}
			if test.failed != nil {
				ctrl.failed = test.failed
			}
			if len(test.inactive.buf) > 0 {
				ctrl.inactive = test.inactive
			}

			plan, store, resp := ctrl.getPlanOrResponse(context.Background(), "test", "instID", "ref")

			if test.wantEmpty {
				if plan != nil || store != nil || resp != nil {
					t.Error("expected all nil for inactive instruction")
				}
				return
			}

			if test.wantErr {
				if resp == nil {
					t.Fatal("expected non-nil error response")
				}
				if resp.Error == "" {
					t.Error("expected error in response")
				}
				return
			}

			if plan == nil {
				t.Error("expected non-nil plan")
			}
		})
	}
}
