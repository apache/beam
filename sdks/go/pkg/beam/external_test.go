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

package beam

import (
	"bytes"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

func TestExternalTagged(t *testing.T) {
	urn := "my::test::urn"
	payload := []byte("expected_payload")
	inTag := "inTag"
	outTag := "outTag"

	p := NewPipeline()
	s := p.Root()

	col := Impulse(s)
	ft := typex.New(reflectx.ByteSlice)
	outMap := ExternalTagged(s, urn, payload, map[string]PCollection{inTag: col}, map[string]FullType{outTag: ft}, true)

	// Validate the in-construction PCollection types for ExternalTagged.
	if len(outMap) != 1 {
		t.Fatalf("ExternalTagged = %v, want 1 output with tag %q", outMap, outTag)
	}
	out, ok := outMap[outTag]
	if !ok {
		t.Fatalf("ExternalTagged = %v, want 1 output with tag %q", outMap, outTag)
	}
	if !typex.IsEqual(out.Type(), ft) {
		t.Fatalf("ExternalTagged()[%q].Type() = %v, want %v", outTag, out.Type(), ft)
	}

	// Validate the post-construction pipeline protocol buffer contents.
	edges, _, err := p.Build()
	if err != nil {
		t.Fatalf("Pipeline couldn't build: %v", err)
	}
	pb, err := graphx.Marshal(edges, &graphx.Options{Environment: &pipepb.Environment{}})
	if err != nil {
		t.Fatalf("Couldn't graphx.Marshal edges: %v", err)
	}
	components := pb.GetComponents()
	transforms := components.GetTransforms()

	foundExternalTagged := false
	for _, transform := range transforms {
		spec := transform.GetSpec()
		if !strings.Contains(spec.GetUrn(), urn) {
			continue
		}
		foundExternalTagged = true
		if !bytes.Equal(spec.GetPayload(), payload) {
			t.Errorf("Payload value: got %v, want %v", spec.GetPayload(), payload)
		}
		if got, want := len(transform.GetInputs()), 1; got != want {
			t.Errorf("transform.Inputs has size %v, want %v: %v", got, want, transform.GetInputs())
		} else if _, ok := transform.GetInputs()[inTag]; !ok {
			t.Errorf("transform.Inputs doesn't contain %v tag, got %v", inTag, transform.GetInputs())
		}

		if got, want := len(transform.GetOutputs()), 1; got != want {
			t.Errorf("transform.Outputs has size %v, want %v: %v", got, want, transform.GetOutputs())
		} else if _, ok := transform.GetOutputs()[outTag]; !ok {
			t.Errorf("transform.Outputs doesn't contain %v tag, got %v", outTag, transform.GetOutputs())
		}
		break
	}
	if !foundExternalTagged {
		t.Errorf("Couldn't find PTransform with urn %q in graph %v", urn, transforms)
	}
}
