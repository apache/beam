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

// Package pipelinex contains utilities for manipulating Beam proto pipelines.
// The utilities generally uses shallow copies and do not mutate their inputs.
package pipelinex

import (
	"fmt"
	"sort"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
)

// Update merges a pipeline with the given components, which may add, replace
// or delete its values. It returns the merged pipeline. The input is not
// modified.
func Update(p *pipepb.Pipeline, values *pipepb.Components) (*pipepb.Pipeline, error) {
	ret := shallowClonePipeline(p)
	reflectx.UpdateMap(ret.Components.Transforms, values.Transforms)
	reflectx.UpdateMap(ret.Components.Pcollections, values.Pcollections)
	reflectx.UpdateMap(ret.Components.WindowingStrategies, values.WindowingStrategies)
	reflectx.UpdateMap(ret.Components.Coders, values.Coders)
	reflectx.UpdateMap(ret.Components.Environments, values.Environments)
	return Normalize(ret)
}

// Normalize recomputes derivative information in the pipeline, such
// as roots and input/output for composite transforms. It also
// ensures that unique names are so and topologically sorts each
// subtransform list.
func Normalize(p *pipepb.Pipeline) (*pipepb.Pipeline, error) {
	if len(p.GetComponents().GetTransforms()) == 0 {
		return nil, errors.New("empty pipeline")
	}

	ret := shallowClonePipeline(p)
	ret.Components.Transforms = ensureUniqueNames(ret.Components.Transforms)
	ret.Components.Transforms = computeCompositeInputOutput(ret.Components.Transforms)
	ret.RootTransformIds = computeRoots(ret.Components.Transforms)
	return ret, nil
}

// TrimCoders returns the transitive closure of the given coders ids.
func TrimCoders(coders map[string]*pipepb.Coder, ids ...string) map[string]*pipepb.Coder {
	ret := make(map[string]*pipepb.Coder)
	for _, id := range ids {
		walkCoders(coders, ret, id)
	}
	return ret
}

func walkCoders(coders, accum map[string]*pipepb.Coder, id string) {
	if _, ok := accum[id]; ok {
		return // already visited
	}

	c := coders[id]
	accum[id] = c
	for _, sub := range c.ComponentCoderIds {
		walkCoders(coders, accum, sub)
	}
}

// computeRoots returns the root (top-level) transform IDs.
func computeRoots(xforms map[string]*pipepb.PTransform) []string {
	var roots []string
	parents := makeParentMap(xforms)
	for id := range xforms {
		if _, ok := parents[id]; !ok {
			// Transforms that do not have a parent is a root
			roots = append(roots, id)
		}
	}
	return TopologicalSort(xforms, roots)
}

func makeParentMap(xforms map[string]*pipepb.PTransform) map[string]string {
	parent := make(map[string]string)
	for id, t := range xforms {
		for _, key := range t.Subtransforms {
			parent[key] = id
		}
	}
	return parent
}

// computeCompositeInputOutput computes the derived input/output maps
// for composite transforms.
func computeCompositeInputOutput(xforms map[string]*pipepb.PTransform) map[string]*pipepb.PTransform {
	ret := reflectx.ShallowClone(xforms).(map[string]*pipepb.PTransform)

	seen := make(map[string]bool)
	for id := range xforms {
		walk(id, ret, seen)
	}
	return ret
}

// walk traverses the structure recursively to compute the input/output
// maps of composite transforms. Update the transform map.
func walk(id string, ret map[string]*pipepb.PTransform, seen map[string]bool) {
	t := ret[id]
	if seen[id] || len(t.Subtransforms) == 0 {
		return
	}

	// Compute the input/output for this composite:
	//    inputs  := U(subinputs)\U(suboutputs)
	//    outputs := U(suboutputs)\U(subinputs)
	// where U is set union and \ is set subtraction.

	in := make(map[string]bool)
	out := make(map[string]bool)
	local := map[string]bool{id: true}
	for _, sid := range t.Subtransforms {
		walk(sid, ret, seen)
		inout(ret[sid], in, out)
		local[sid] = true
	}

	// At this point, we know all the inputs and outputs of this composite.
	// However, outputs in this PTransform can also be used by PTransforms
	// external to this composite. So we must check the inputs in the rest of
	// the graph, and ensure they're counted.
	extIn := make(map[string]bool)
	externalIns(local, ret, extIn, out)

	upd := ShallowClonePTransform(t)
	upd.Inputs = diff(in, out)
	upd.Outputs = diffAndMerge(out, in, extIn)
	upd.Subtransforms = TopologicalSort(ret, upd.Subtransforms)

	ret[id] = upd
	seen[id] = true
}

// diff computes A\B and returns its keys as an identity map.
func diff(a, b map[string]bool) map[string]string {
	if len(a) == 0 {
		return nil
	}
	ret := make(map[string]string)
	for key := range a {
		if !b[key] {
			ret[key] = key
		}
	}
	if len(ret) == 0 {
		return nil
	}
	return ret
}

// inout adds the input and output pcollection ids to the accumulators.
func inout(transform *pipepb.PTransform, in, out map[string]bool) {
	for _, col := range transform.GetInputs() {
		in[col] = true
	}
	for _, col := range transform.GetOutputs() {
		out[col] = true
	}
}
func diffAndMerge(out, in, extIn map[string]bool) map[string]string {
	ret := diff(out, in)
	for key := range extIn {
		if ret == nil {
			ret = make(map[string]string)
		}
		ret[key] = key
	}
	return ret
}

// externalIns checks the unseen non-composite graph
func externalIns(counted map[string]bool, xforms map[string]*pipepb.PTransform, extIn, out map[string]bool) {
	for id, pt := range xforms {
		// Ignore other composites or already counted transforms.
		if counted[id] || len(pt.GetSubtransforms()) != 0 {
			continue
		}
		// Check this PTransform's inputs for anything output by something
		// the current composite.
		for col := range out {
			for _, incol := range pt.GetInputs() {
				if col == incol {
					extIn[col] = true
				}
			}
		}
	}
}

// ensureUniqueNames ensures that each name is unique. Any conflict is
// resolved by adding '1, '2, etc to the name.
func ensureUniqueNames(xforms map[string]*pipepb.PTransform) map[string]*pipepb.PTransform {
	ret := reflectx.ShallowClone(xforms).(map[string]*pipepb.PTransform)

	// Sort the transforms to make to make renaming deterministic.
	var ordering []string
	for id := range xforms {
		ordering = append(ordering, id)
	}
	sort.Strings(ordering)

	seen := make(map[string]bool)
	for _, id := range ordering {
		t := xforms[id]
		name := findFreeName(seen, t.UniqueName)
		seen[name] = true

		if name != t.UniqueName {
			upd := ShallowClonePTransform(t)
			upd.UniqueName = name
			ret[id] = upd
		}
	}
	return ret
}

func findFreeName(seen map[string]bool, name string) string {
	if !seen[name] {
		return name
	}
	for i := 1; ; i++ {
		next := fmt.Sprintf("%v'%v", name, i)
		if !seen[next] {
			return next
		}
	}
}
