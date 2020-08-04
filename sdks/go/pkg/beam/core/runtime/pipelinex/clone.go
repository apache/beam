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
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
)

func shallowClonePipeline(p *pipepb.Pipeline) *pipepb.Pipeline {
	ret := &pipepb.Pipeline{
		Components:   shallowCloneComponents(p.GetComponents()),
		Requirements: reflectx.ShallowClone(p.GetRequirements()).([]string),
	}
	ret.RootTransformIds, _ = reflectx.ShallowClone(p.GetRootTransformIds()).([]string)
	return ret
}

func shallowCloneComponents(comp *pipepb.Components) *pipepb.Components {
	ret := &pipepb.Components{}
	ret.Transforms, _ = reflectx.ShallowClone(comp.GetTransforms()).(map[string]*pipepb.PTransform)
	ret.Pcollections, _ = reflectx.ShallowClone(comp.GetPcollections()).(map[string]*pipepb.PCollection)
	ret.WindowingStrategies, _ = reflectx.ShallowClone(comp.GetWindowingStrategies()).(map[string]*pipepb.WindowingStrategy)
	ret.Coders, _ = reflectx.ShallowClone(comp.GetCoders()).(map[string]*pipepb.Coder)
	ret.Environments, _ = reflectx.ShallowClone(comp.GetEnvironments()).(map[string]*pipepb.Environment)
	return ret
}

// ShallowClonePTransform makes a shallow copy of the given PTransform.
func ShallowClonePTransform(t *pipepb.PTransform) *pipepb.PTransform {
	if t == nil {
		return nil
	}

	ret := &pipepb.PTransform{
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
func ShallowCloneParDoPayload(p *pipepb.ParDoPayload) *pipepb.ParDoPayload {
	if p == nil {
		return nil
	}

	ret := &pipepb.ParDoPayload{
		DoFn:               p.DoFn,
		RestrictionCoderId: p.RestrictionCoderId,
	}
	ret.SideInputs, _ = reflectx.ShallowClone(p.SideInputs).(map[string]*pipepb.SideInput)
	ret.StateSpecs, _ = reflectx.ShallowClone(p.StateSpecs).(map[string]*pipepb.StateSpec)
	ret.TimerFamilySpecs, _ = reflectx.ShallowClone(p.TimerFamilySpecs).(map[string]*pipepb.TimerFamilySpec)
	return ret
}

// ShallowCloneSideInput makes a shallow copy of the given SideInput.
func ShallowCloneSideInput(p *pipepb.SideInput) *pipepb.SideInput {
	if p == nil {
		return nil
	}
	var ret pipepb.SideInput
	ret = *p
	return &ret
}

// ShallowCloneFunctionSpec makes a shallow copy of the given FunctionSpec.
func ShallowCloneFunctionSpec(p *pipepb.FunctionSpec) *pipepb.FunctionSpec {
	if p == nil {
		return nil
	}
	var ret pipepb.FunctionSpec
	ret = *p
	return &ret
}
