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

package xlangx

import (
	"context"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
)

func TestRegistry(t *testing.T) {
	var handler = func(context.Context, *HandlerParams) (*jobpb.ExpansionResponse, error) {
		return nil, nil
	}
	toPtr := func(fn any) uintptr {
		return reflect.ValueOf(handler).Pointer()
	}

	reg := newRegistry()
	checkLookup := func(t *testing.T, urn, addr, wantCfg string, wantHandler HandlerFunc, debugName string) {
		t.Helper()
		h, cfg := reg.getHandlerFunc(urn, addr)
		if got, want := cfg, wantCfg; got != want {
			t.Errorf("getHandlerFunc(%v, %v) = %q as config, want %q", urn, addr, got, want)
		}
		if got, want := toPtr(h), toPtr(wantHandler); got != want {
			t.Errorf("getHandlerFunc(%v, %v) = %v as handler uintptr, want %v (%v)", urn, addr, got, want, debugName)
		}
	}
	urn, addr := "myurn", "nohandler"

	// Check default fallback.
	checkLookup(t, urn, addr, addr, QueryExpansionService, "QueryExpansionService")

	// Check that the automated expansion service check is correct.
	// Do this before we register URNs later in the test.
	auto := autoJavaNamespace + Separator + ":sdks:somelanguage:expansion"
	checkLookup(t, urn, auto, auto, QueryAutomatedExpansionService, "QueryAutomatedExpansionService")

	// Check registration.
	addr = "handler"
	if err := reg.RegisterHandler(addr, handler); err != nil {
		t.Errorf("RegisterHandler(%v, %v) error:  %v", addr, toPtr(handler), err)
	}
	checkLookup(t, urn, addr, "", handler, "Registered Handler")
	arbitrary := "arbitrary configuration: right?"
	checkLookup(t, urn, addr+Separator+arbitrary, arbitrary, handler, "Registered Handler")

	// Check URN override
	overrideAddr := "http://localhost:6375309"
	if err := reg.RegisterOverrideForUrn(urn, overrideAddr); err != nil {
		t.Errorf("RegisterOverrideForUrn(%v, %v) error:  %v", urn, overrideAddr, err)
	}
	// Checks that even with a registered handler, we get the query service instead.
	checkLookup(t, urn, "handler:wow", overrideAddr, QueryExpansionService, "QueryExpansionService")

	overrideAddr = "handler:wow"
	if err := reg.RegisterOverrideForUrn(urn, overrideAddr); err != nil {
		t.Errorf("RegisterOverrideForUrn(%v, %v) error:  %v", urn, overrideAddr, err)
	}
	checkLookup(t, urn, "ignored address", "wow", handler, "Registered Handler")

	required := "http://localhost:6375309"
	// Check Require works, ignoring the previous urn overrides.
	checkLookup(t, urn, Require(required), required, QueryExpansionService, "QueryExpansionService")
}

func TestUseAutomatedExpansionService(t *testing.T) {
	target := ":sdks:java:extensions:someservice:shadowJar"
	if got, want := UseAutomatedJavaExpansionService(target), autoJavaNamespace+Separator+target; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestHandlerParamsInputsOutputsOrder(t *testing.T) {
	in0 := &graph.Node{}
	in1 := &graph.Node{}
	in2 := &graph.Node{}
	out0 := &graph.Node{}
	out1 := &graph.Node{}

	p := &HandlerParams{
		Req: &jobpb.ExpansionRequest{Namespace: "ns"},
		edge: &graph.MultiEdge{
			Input: []*graph.Inbound{
				{From: in0},
				{From: in1},
				{From: in2},
			},
			Output: []*graph.Outbound{
				{To: out0},
				{To: out1},
			},
		},
		ext: &graph.ExternalTransform{
			InputsMap: map[string]int{
				"table_c": 2,
				"table_a": 0,
				"table_b": 1,
			},
			OutputsMap: map[string]int{
				"out_b": 1,
				"out_a": 0,
			},
		},
	}

	for iter := 0; iter < 20; iter++ {
		ins := p.Inputs()
		wantInLocals := []string{"table_a", "table_b", "table_c"}
		for i, wantLocal := range wantInLocals {
			if ins[i].Index != i || ins[i].Local != wantLocal {
				t.Fatalf("iter %d: Inputs()[%d] = {Index: %d, Local: %q}, want {Index: %d, Local: %q}",
					iter, i, ins[i].Index, ins[i].Local, i, wantLocal)
			}
		}

		outs := p.Outputs()
		wantOutLocals := []string{"out_a", "out_b"}
		for i, wantLocal := range wantOutLocals {
			if outs[i].Index != i || outs[i].Local != wantLocal {
				t.Fatalf("iter %d: Outputs()[%d] = {Index: %d, Local: %q}, want {Index: %d, Local: %q}",
					iter, i, outs[i].Index, outs[i].Local, i, wantLocal)
			}
		}
	}

	// Verify that a subset/sparse OutputsMap (such as when SetOutputCoder
	// rewrites OutputsMap to a single entry at index > 0) does not panic.
	p.ext.OutputsMap = map[string]int{graph.UnnamedOutputTag: 1}
	outs := p.Outputs()
	if len(outs) != 1 || outs[0].Index != 1 || outs[0].Local != graph.UnnamedOutputTag {
		t.Fatalf("Outputs() with non-zero index = %v, want single entry at Index 1", outs)
	}
}
