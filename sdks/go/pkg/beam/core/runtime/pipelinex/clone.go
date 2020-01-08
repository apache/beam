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
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
)

func shallowClonePipeline(p *pb.Pipeline) *pb.Pipeline {
	ret := &pb.Pipeline{
		Components: shallowCloneComponents(p.GetComponents()),
	}
	ret.RootTransformIds, _ = reflectx.ShallowClone(p.GetRootTransformIds()).([]string)
	return ret
}

func shallowCloneComponents(comp *pb.Components) *pb.Components {
	ret := &pb.Components{}
	ret.Transforms, _ = reflectx.ShallowClone(comp.GetTransforms()).(map[string]*pb.PTransform)
	ret.Pcollections, _ = reflectx.ShallowClone(comp.GetPcollections()).(map[string]*pb.PCollection)
	ret.WindowingStrategies, _ = reflectx.ShallowClone(comp.GetWindowingStrategies()).(map[string]*pb.WindowingStrategy)
	ret.Coders, _ = reflectx.ShallowClone(comp.GetCoders()).(map[string]*pb.Coder)
	ret.Environments, _ = reflectx.ShallowClone(comp.GetEnvironments()).(map[string]*pb.Environment)
	return ret
}

// ShallowClonePTransform makes a shallow copy of the given PTransform.
func ShallowClonePTransform(t *pb.PTransform) *pb.PTransform {
	if t == nil {
		return nil
	}

	ret := &pb.PTransform{
		UniqueName:  t.UniqueName,
		Spec:        t.Spec,
		DisplayData: t.DisplayData,
	}
	ret.Subtransforms, _ = reflectx.ShallowClone(t.Subtransforms).([]string)
	ret.Inputs, _ = reflectx.ShallowClone(t.Inputs).(map[string]string)
	ret.Outputs, _ = reflectx.ShallowClone(t.Outputs).(map[string]string)
	ret.EnvironmentId = t.EnvironmentId
	return ret
}

// ShallowCloneParDoPayload makes a shallow copy of the given ParDoPayload.
func ShallowCloneParDoPayload(p *pb.ParDoPayload) *pb.ParDoPayload {
	if p == nil {
		return nil
	}

	ret := &pb.ParDoPayload{
		DoFn:               p.DoFn,
		Splittable:         p.Splittable,
		RestrictionCoderId: p.RestrictionCoderId,
	}
	ret.Parameters, _ = reflectx.ShallowClone(p.Parameters).([]*pb.Parameter)
	ret.SideInputs, _ = reflectx.ShallowClone(p.SideInputs).(map[string]*pb.SideInput)
	ret.StateSpecs, _ = reflectx.ShallowClone(p.StateSpecs).(map[string]*pb.StateSpec)
	ret.TimerSpecs, _ = reflectx.ShallowClone(p.TimerSpecs).(map[string]*pb.TimerSpec)
	return ret
}

// ShallowCloneSideInput makes a shallow copy of the given SideInput.
func ShallowCloneSideInput(p *pb.SideInput) *pb.SideInput {
	if p == nil {
		return nil
	}
	var ret pb.SideInput
	ret = *p
	return &ret
}

// ShallowCloneFunctionSpec makes a shallow copy of the given FunctionSpec.
func ShallowCloneFunctionSpec(p *pb.FunctionSpec) *pb.FunctionSpec {
	if p == nil {
		return nil
	}
	var ret pb.FunctionSpec
	ret = *p
	return &ret
}
