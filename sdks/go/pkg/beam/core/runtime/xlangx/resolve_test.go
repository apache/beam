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
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/protobuf/proto"
)

func createExternalEdge(typeUrn string, typePayload []byte) *graph.MultiEdge {
	env := map[string]*pipepb.Environment{
		"env_java": {
			Dependencies: []*pipepb.ArtifactInformation{
				{
					TypeUrn:     typeUrn,
					TypePayload: typePayload,
				},
			},
		},
	}
	return &graph.MultiEdge{
		Op: graph.External,
		External: &graph.ExternalTransform{
			Expanded: &graph.ExpandedTransform{
				Components: &pipepb.Components{
					Environments: env,
				},
			},
		},
	}
}

func TestUpdateFileArtifactWithUrlPath(t *testing.T) {
	payload, _ := proto.Marshal(&pipepb.ArtifactFilePayload{
		Path: "gs://dummy"})
	e := createExternalEdge("beam:artifact:type:file:v1", payload)
	UpdateArtifactTypeFromFileToURL([]*graph.MultiEdge{e})
	expected := createExternalEdge("beam:artifact:type:url:v1", payload)

	if !proto.Equal(
		e.External.Expanded.Components.(*pipepb.Components),
		expected.External.Expanded.Components.(*pipepb.Components)) {
		t.Errorf("Mismatched output %v != %v",
			e.External.Expanded.Components.(*pipepb.Components),
			expected.External.Expanded.Components.(*pipepb.Components))
	}
}

func TestUpdateFileArtifactWithLocalPath(t *testing.T) {
	payload, _ := proto.Marshal(&pipepb.ArtifactFilePayload{
		Path: "/tmp/artifact/dummy"})
	e := createExternalEdge("beam:artifact:type:file:v1", payload)
	UpdateArtifactTypeFromFileToURL([]*graph.MultiEdge{e})
	expected := createExternalEdge("beam:artifact:type:file:v1", payload)

	if !proto.Equal(
		e.External.Expanded.Components.(*pipepb.Components),
		expected.External.Expanded.Components.(*pipepb.Components)) {
		t.Errorf("Mismatched output %v != %v",
			e.External.Expanded.Components.(*pipepb.Components),
			expected.External.Expanded.Components.(*pipepb.Components))
	}
}
