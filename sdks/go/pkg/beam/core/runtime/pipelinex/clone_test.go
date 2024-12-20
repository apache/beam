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

package pipelinex

import (
	"testing"

	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
)

func TestShallowClonePTransform(t *testing.T) {
	tests := []*pipepb.PTransform{
		{},
		pipepb.PTransform_builder{UniqueName: "a"}.Build(),
		pipepb.PTransform_builder{Spec: pipepb.FunctionSpec_builder{Urn: "foo"}.Build()}.Build(),
		pipepb.PTransform_builder{Subtransforms: []string{"a", "b"}}.Build(),
		pipepb.PTransform_builder{Inputs: map[string]string{"a": "b"}}.Build(),
		pipepb.PTransform_builder{Outputs: map[string]string{"a": "b"}}.Build(),
	}

	for _, test := range tests {
		actual := ShallowClonePTransform(test)
		if !cmp.Equal(actual, test, cmp.Comparer(proto.Equal)) {
			t.Errorf("ShallowClonePCollection(%v) = %v, want id", test, actual)
		}
	}
}
