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

package dataflowlib

import (
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/pipelinex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/golang/protobuf/proto"
)

// Fixup proto pipeline with Dataflow quirks.
func Fixup(p *pb.Pipeline) (*pb.Pipeline, error) {
	upd := make(map[string]*pb.PTransform)

	for id, t := range p.GetComponents().GetTransforms() {
		if t.GetSpec().GetUrn() != graphx.URNParDo {
			continue
		}
		var payload pb.ParDoPayload
		if err := proto.Unmarshal(t.GetSpec().GetPayload(), &payload); err != nil {
			continue // ignore: unexpected payload
		}
		if len(payload.SideInputs) == 0 {
			continue
		}

		// ParDo w/ side input. Fixup URN.

		fixedPayload := pipelinex.ShallowCloneParDoPayload(&payload)
		for k, v := range payload.SideInputs {
			fixedV := pipelinex.ShallowCloneSideInput(v)
			fixedV.AccessPattern = pipelinex.ShallowCloneFunctionSpec(v.AccessPattern)
			fixedV.AccessPattern.Urn = "urn:beam:sideinput:materialization:multimap:0.1"

			fixedPayload.SideInputs[k] = fixedV
		}
		fixed := pipelinex.ShallowClonePTransform(t)
		fixed.Spec = pipelinex.ShallowCloneFunctionSpec(t.Spec)
		fixed.Spec.Payload = protox.MustEncode(fixedPayload)

		upd[id] = fixed
	}
	return pipelinex.Update(p, &pb.Components{Transforms: upd})
}
