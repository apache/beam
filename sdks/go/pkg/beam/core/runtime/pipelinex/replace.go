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
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/protobuf/proto"
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

// IdempotentNormalize determines whether to use the idempotent version
// of ensureUniqueNames or the legacy version.
// TODO(BEAM-12341): Cleanup once nothing depends on the legacy implementation.
var IdempotentNormalize bool = true

// Normalize recomputes derivative information in the pipeline, such
// as roots and input/output for composite transforms. It also
// ensures that unique names are so and topologically sorts each
// subtransform list.
func Normalize(p *pipepb.Pipeline) (*pipepb.Pipeline, error) {
	if len(p.GetComponents().GetTransforms()) == 0 {
		return nil, errors.New("empty pipeline")
	}

	ret := shallowClonePipeline(p)
	if IdempotentNormalize {
		ret.Components.Transforms = ensureUniqueNames(ret.Components.Transforms)
	} else {
		ret.Components.Transforms = ensureUniqueNamesLegacy(ret.Components.Transforms)
	}
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
	// Precompute the transforms that consume each PCollection as input.
	primitiveXformsForInput := make(map[string][]string)
	for id, pt := range xforms {
		if len(pt.GetSubtransforms()) == 0 {
			for _, col := range pt.GetInputs() {
				primitiveXformsForInput[col] = append(primitiveXformsForInput[col], id)
			}
		}
	}

	seen := make(map[string]bool)
	for id := range xforms {
		walk(id, ret, seen, primitiveXformsForInput)
	}
	return ret
}

// walk traverses the structure recursively to compute the input/output
// maps of composite transforms. Update the transform map.
func walk(id string, ret map[string]*pipepb.PTransform, seen map[string]bool, primitiveXformsForInput map[string][]string) {
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
		walk(sid, ret, seen, primitiveXformsForInput)
		inout(ret[sid], in, out)
		local[sid] = true
	}

	// At this point, we know all the inputs and outputs of this composite.
	// However, outputs in this PTransform can also be used by PTransforms
	// external to this composite. So we must check the inputs in the rest of
	// the graph, and ensure they're counted.
	extIn := make(map[string]bool)
	externalIns(local, primitiveXformsForInput, extIn, out)

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
func externalIns(counted map[string]bool, primitiveXformsForInput map[string][]string, extIn, out map[string]bool) {
	// For this composite's output PCollections.
	for col := range out {
		// See if any transforms are using it.
		for _, id := range primitiveXformsForInput[col] {
			if !counted[id] {
				// And if they're part of the composite
				// Ensure the collections is in the set of outputs for the composite.
				extIn[col] = true
			}
		}
	}
}

type idSorted []string

func (s idSorted) Len() int {
	return len(s)
}
func (s idSorted) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Go SDK ids for transforms are "e#" or "s#" and we want to
// sort them properly at the root level at least. Cross lang
// transforms or expanded nodes (like CoGBK) don't follow this
// format should be sorted lexicographically, but are wrapped in
// a composite ptransform meaning they're compared to fewer
// transforms.
var idParseExp = regexp.MustCompile(`(\D*)(\d*)`)

func (s idSorted) Less(i, j int) bool {
	// We want to sort alphabetically by id prefix
	// and numerically by id suffix.
	// Otherwise, values are compared lexicographically.
	iM := idParseExp.FindStringSubmatch(s[i])
	jM := idParseExp.FindStringSubmatch(s[j])
	if iM == nil || jM == nil {
		return s[i] < s[j]
	}
	// check if the letters match.
	if iM[1] < jM[1] {
		return true
	}
	if iM[1] > jM[1] {
		return false
	}
	// The letters match, check the numbers.
	// We can ignore the errors here due to the regex check.
	iN, _ := strconv.Atoi(iM[2])
	jN, _ := strconv.Atoi(jM[2])
	return iN < jN
}

func separateCompsAndLeaves(xforms map[string]*pipepb.PTransform) (comp, leaf []string) {
	var cs, ls idSorted
	for id, pt := range xforms {
		if len(pt.GetSubtransforms()) == 0 {
			// No subtransforms, it's a leaf!
			ls = append(ls, id)
		} else {
			// Subtransforms, it's a composite
			cs = append(cs, id)
		}
	}
	// Sort the transforms to make to make renaming deterministic.
	sort.Sort(cs)
	sort.Sort(ls)
	return []string(cs), []string(ls)
}

// ensureUniqueNames ensures that each name is unique.
//
// Subtransforms are prefixed with the names of their parent, separated by a '/'.
// Any conflict is resolved by adding '1, '2, etc to the name.
func ensureUniqueNames(xforms map[string]*pipepb.PTransform) map[string]*pipepb.PTransform {
	ret := reflectx.ShallowClone(xforms).(map[string]*pipepb.PTransform)

	comp, leaf := separateCompsAndLeaves(xforms)
	parentLookup := make(map[string]string) // childID -> parentID
	for _, parentID := range comp {
		t := xforms[parentID]
		children := t.GetSubtransforms()
		for _, childID := range children {
			parentLookup[childID] = parentID
		}
	}

	parentNameCache := make(map[string]string) // parentID -> parentName
	seen := make(map[string]bool)
	// Closure to to make the names unique so we can handle all the parent ids first.
	uniquify := func(id string) string {
		t := xforms[id]
		base := path.Base(t.GetUniqueName())
		var prefix string
		if parentID, ok := parentLookup[id]; ok {
			prefix = getParentName(parentNameCache, parentLookup, parentID, xforms)
		}
		base = prefix + base
		name := findFreeName(seen, base)
		seen[name] = true

		if name != t.UniqueName {
			upd := ShallowClonePTransform(t)
			upd.UniqueName = name
			ret[id] = upd
		}
		return name
	}
	for _, id := range comp {
		name := uniquify(id)
		parentNameCache[id] = name + "/"
	}
	for _, id := range leaf {
		uniquify(id)
	}
	return ret
}

func getParentName(nameCache, parentLookup map[string]string, parentID string, xforms map[string]*pipepb.PTransform) string {
	if name, ok := nameCache[parentID]; ok {
		return name
	}
	var parts []string
	curID := parentID
	for {
		t := xforms[curID]
		// Construct composite names from scratch if the parent's not
		// already in the cache. Otherwise there's a risk of errors from
		// not following topological orderings.
		parts = append(parts, path.Base(t.GetUniqueName()))
		if pid, ok := parentLookup[curID]; ok {
			curID = pid
			continue
		}
		break
	}

	// reverse the parts so parents are first.
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	name := strings.Join(parts, "/") + "/"
	nameCache[parentID] = name
	return name
}

// ensureUniqueNamesLegacy ensures that each name is unique. Any conflict is
// resolved by adding '1, '2, etc to the name.
// Older version that wasn't idempotent. Sticking around for temporary migration purposes.
func ensureUniqueNamesLegacy(xforms map[string]*pipepb.PTransform) map[string]*pipepb.PTransform {
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

// ApplySdkImageOverrides takes a pipeline and a map of patterns to overrides,
// and proceeds to replace matching ContainerImages in any Environments
// present in the pipeline. Each environment is expected to match at most one
// pattern. If an environment matches two or more it is arbitrary which
// pattern will be applied.
func ApplySdkImageOverrides(p *pipepb.Pipeline, patterns map[string]string) error {
	if len(patterns) == 0 {
		return nil
	}

	// Precompile all patterns as regexes.
	regexes := make(map[*regexp.Regexp]string, len(patterns))
	for p, r := range patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return err
		}
		regexes[re] = r
	}

	for _, env := range p.GetComponents().GetEnvironments() {
		var payload pipepb.DockerPayload
		if err := proto.Unmarshal(env.GetPayload(), &payload); err != nil {
			return err
		}
		oldImg := payload.GetContainerImage()
		for re, replacement := range regexes {
			newImg := re.ReplaceAllLiteralString(oldImg, replacement)
			if newImg != oldImg {
				payload.ContainerImage = newImg
				pl, err := proto.Marshal(&payload)
				if err != nil {
					return err
				}
				env.Payload = pl
				break // Apply at most one override to each environment.
			}
		}
	}
	return nil
}
