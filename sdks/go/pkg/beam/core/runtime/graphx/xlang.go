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

package graphx

import (
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
)

func ExpandedComponents(exp *graph.ExpandedTransform) *pipepb.Components {
	if c, ok := exp.Components.(*pipepb.Components); ok {
		return c
	}
	panic(errors.Errorf("malformed components; %v lacks a conforming pipeline component", exp))
}

func ExpandedTransform(exp *graph.ExpandedTransform) *pipepb.PTransform {
	if t, ok := exp.Transform.(*pipepb.PTransform); ok {
		return t
	}
	panic(errors.Errorf("malformed transform; %v lacks a conforming pipeline ptransform", exp))
}

func ExternalInputs(e *graph.MultiEdge) map[string]*graph.Node {
	inputs := make(map[string]*graph.Node)

	for tag, id := range e.External.InputsMap {
		inputs[tag] = e.Input[id].From
	}
	return inputs
}

func ExternalOutputs(e *graph.MultiEdge) map[string]*graph.Node {
	outputs := make(map[string]*graph.Node)

	for tag, id := range e.External.OutputsMap {
		outputs[tag] = e.Output[id].To
	}
	return outputs
}
