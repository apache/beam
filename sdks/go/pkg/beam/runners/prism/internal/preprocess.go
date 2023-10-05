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

package internal

import (
	"fmt"
	"sort"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/pipelinex"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slog"
	"google.golang.org/protobuf/encoding/prototext"
)

// transformPreparer is an interface for handling different urns in the preprocessor
// largely for exchanging transforms for others, to be added to the complete set of
// components in the pipeline.
type transformPreparer interface {
	// PrepareUrns returns the Beam URNs that this handler deals with for preprocessing.
	PrepareUrns() []string
	// PrepareTransform takes a PTransform proto and returns a set of new Components, and a list of
	// transformIDs leaves to remove and ignore from graph processing.
	PrepareTransform(tid string, t *pipepb.PTransform, comps *pipepb.Components) (*pipepb.Components, []string)
}

// preprocessor retains configuration for preprocessing the
// graph, such as special handling for lifted combiners or
// other configuration.
type preprocessor struct {
	transformPreparers map[string]transformPreparer
}

func newPreprocessor(preps []transformPreparer) *preprocessor {
	preparers := map[string]transformPreparer{}
	for _, prep := range preps {
		for _, urn := range prep.PrepareUrns() {
			preparers[urn] = prep
		}
	}
	return &preprocessor{
		transformPreparers: preparers,
	}
}

// preProcessGraph takes the graph and preprocesses for consumption in bundles.
// The output is the topological sort of the transform ids.
//
// These are how transforms are related in graph form, but not the specific bundles themselves, which will come later.
//
// Handles awareness of composite transforms and similar. Ultimately, after this point
// the graph stops being a hypergraph, with composite transforms being treated as
// "leaves" downstream as needed.
//
// This is where Combines become lifted (if it makes sense, or is configured), and similar behaviors.
func (p *preprocessor) preProcessGraph(comps *pipepb.Components) []*stage {
	ts := comps.GetTransforms()

	// TODO move this out of this part of the pre-processor?
	leaves := map[string]struct{}{}
	ignore := map[string]struct{}{}
	for tid, t := range ts {
		if _, ok := ignore[tid]; ok {
			continue
		}

		spec := t.GetSpec()
		if spec == nil {
			// Most composites don't have specs.
			slog.Debug("transform is missing a spec",
				slog.Group("transform", slog.String("ID", tid), slog.String("name", t.GetUniqueName())))
			continue
		}

		// Composite Transforms basically means needing to remove the "leaves" from the
		// handling set, and producing the new sub component transforms. The top level
		// composite should have enough information to produce the new sub transforms.
		// In particular, the inputs and outputs need to all be connected and matched up
		// so the topological sort still works out.
		h := p.transformPreparers[spec.GetUrn()]
		if h == nil {

			// If there's an unknown urn, and it's not composite, simply add it to the leaves.
			if len(t.GetSubtransforms()) == 0 {
				leaves[tid] = struct{}{}
			} else {
				slog.Info("composite transform has unknown urn",
					slog.Group("transform", slog.String("ID", tid),
						slog.String("name", t.GetUniqueName()),
						slog.String("urn", spec.GetUrn())))
			}
			continue
		}

		subs, toRemove := h.PrepareTransform(tid, t, comps)

		// Clear out unnecessary leaves from this composite for topological sort handling.
		for _, key := range toRemove {
			ignore[key] = struct{}{}
			delete(leaves, key)
		}

		// ts should be a clone, so we should be able to add new transforms into the map.
		for tid, t := range subs.GetTransforms() {
			leaves[tid] = struct{}{}
			ts[tid] = t
		}
		for cid, c := range subs.GetCoders() {
			comps.GetCoders()[cid] = c
		}
		for nid, n := range subs.GetPcollections() {
			comps.GetPcollections()[nid] = n
		}
		// It's unlikely for these to change, but better to handle them now, to save a headache later.
		for wid, w := range subs.GetWindowingStrategies() {
			comps.GetWindowingStrategies()[wid] = w
		}
		for envid, env := range subs.GetEnvironments() {
			comps.GetEnvironments()[envid] = env
		}
	}

	// Extract URNs for the given transform.

	keptLeaves := maps.Keys(leaves)
	sort.Strings(keptLeaves)
	topological := pipelinex.TopologicalSort(ts, keptLeaves)
	slog.Debug("topological transform ordering", slog.Any("topological", topological))

	// Basic Fusion Behavior
	//
	// Fusion is the practice of executing associated DoFns in the same stage.
	// This often leads to more efficient processing, since costly encode/decode or
	// serialize/deserialize operations can be elided. In Beam, any PCollection can
	// in principle serve as a place for serializing and deserializing elements.
	//
	// In particular, Fusion is a stage for optimizing pipeline execution, and was
	// described in the FlumeJava paper, in section 4.
	// https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/35650.pdf
	//
	// Per the FlumeJava paper, there are two primary opportunities for Fusion,
	// Producer+Consumer fusion and Sibling fusion.
	//
	// Producer+Consumer fusion is when the producer of a PCollection and the consumers of
	// that PCollection are combined into a single stage. Sibling fusion is when two consumers
	// of the same pcollection are fused into the same step. These processes can continue until
	// graph structure or specific transforms dictate that fusion may not proceed futher.
	//
	// Examples of fusion breaks include GroupByKeys, or requiring side inputs to complete
	// processing for downstream processing, since the producer and consumer of side inputs
	// cannot be in the same fused stage.
	//
	// Additionally, at this phase, we can consider different optimizations for execution.
	// For example "Flatten unzipping". In practice, there's no requirement for any stages
	// to have an explicit "Flatten" present in the graph. A flatten can be "unzipped",
	// duplicating the consumming transforms after the flatten, until a subsequent fusion break.
	// This enables additional parallelism by allowing sources to operate in their own independant
	// stages. Beam supports this naturally with the separation of work into independant
	// bundles for execution.

	return defaultFusion(topological, comps)
}

// defaultFusion is the base strategy for prism, that doesn't seek to optimize execution
// with fused stages. Input is the set of leaf nodes we're going to execute, topologically
// sorted, and the pipeline components.
//
// Default fusion behavior: Don't. Prism is intended to test all of Beam, which often
// means for testing purposes, to execute pipelines without optimization.
//
// Special Exception to unfused Go SDK pipelines.
//
// If a transform, after a GBK step, has a single input with a KV<K, Iter<X>> coder
// and a single output O with a KV<K, Iter<Y>> coder, and if then it must be fused with
// the consumers of O.
func defaultFusion(topological []string, comps *pipepb.Components) []*stage {
	var stages []*stage

	// TODO figure out a better place to source the PCol Parents/Consumers analysis
	// so we don't keep repeating it.

	pcolParents, pcolConsumers := computPColFacts(topological, comps)

	// Explicitly list the pcollectionID we want to fuse along.
	fuseWithConsumers := map[string]string{}
	for _, tid := range topological {
		t := comps.GetTransforms()[tid]

		// See if this transform has a single input and output
		if len(t.GetInputs()) != 1 || len(t.GetOutputs()) != 1 {
			continue
		}
		inputID := getOnlyValue(t.GetInputs())
		outputID := getOnlyValue(t.GetOutputs())

		parentLink := pcolParents[inputID]

		parent := comps.GetTransforms()[parentLink.transform]

		// Check if the input source is a GBK
		if parent.GetSpec().GetUrn() != urns.TransformGBK {
			continue
		}

		// Check if the coder is a KV<K, Iter<?>>
		iCID := comps.GetPcollections()[inputID].GetCoderId()
		oCID := comps.GetPcollections()[outputID].GetCoderId()

		if checkForExpandCoderPattern(iCID, oCID, comps) {
			fuseWithConsumers[tid] = outputID
		}
	}

	// Since we iterate in topological order, we're guaranteed to process producers before consumers.
	consumed := map[string]bool{} // Checks if we've already handled a transform already due to fusion.
	for _, tid := range topological {
		if consumed[tid] {
			continue
		}
		stg := &stage{
			transforms: []string{tid},
		}
		// TODO validate that fused stages have the same environment.
		stg.envID = comps.GetTransforms()[tid].EnvironmentId

		stages = append(stages, stg)

		pcolID, ok := fuseWithConsumers[tid]
		if !ok {
			continue
		}
		cs := pcolConsumers[pcolID]

		for _, c := range cs {
			stg.transforms = append(stg.transforms, c.transform)
			consumed[c.transform] = true
		}
	}

	for _, stg := range stages {
		prepareStage(stg, comps, pcolConsumers)
	}
	return stages
}

// computPColFacts computes a map of PCollectionIDs to their parent transforms, and a map of
// PCollectionIDs to their consuming transforms.
func computPColFacts(topological []string, comps *pipepb.Components) (map[string]link, map[string][]link) {
	pcolParents := map[string]link{}
	pcolConsumers := map[string][]link{}

	// Use the topological ids so each PCollection only has a single
	// parent. We've already pruned out composites at this stage.
	for _, tID := range topological {
		t := comps.GetTransforms()[tID]
		for local, global := range t.GetOutputs() {
			pcolParents[global] = link{transform: tID, local: local, global: global}
		}
		for local, global := range t.GetInputs() {
			pcolConsumers[global] = append(pcolConsumers[global], link{transform: tID, local: local, global: global})
		}
	}

	return pcolParents, pcolConsumers
}

// We need to see that both coders have this pattern: KV<K, Iter<?>>
func checkForExpandCoderPattern(in, out string, comps *pipepb.Components) bool {
	isKV := func(id string) bool {
		return comps.GetCoders()[id].GetSpec().GetUrn() == urns.CoderKV
	}
	getComp := func(id string, i int) string {
		return comps.GetCoders()[id].GetComponentCoderIds()[i]
	}
	isIter := func(id string) bool {
		return comps.GetCoders()[id].GetSpec().GetUrn() == urns.CoderIterable
	}
	if !isKV(in) || !isKV(out) {
		return false
	}
	// Are the keys identical?
	if getComp(in, 0) != getComp(out, 0) {
		return false
	}
	// Are both values iterables?
	if isIter(getComp(in, 1)) && isIter(getComp(out, 1)) {
		// If so we have the ExpandCoderPattern from the Go SDK. Hurray!
		return true
	}
	return false
}

// prepareStage does the final pre-processing step for stages:
//
// 1. Determining the single parallel input (may be 0 for impulse stages).
// 2. Determining all outputs to the stages.
// 3. Determining all side inputs.
// 4  validating that no side input is fed by an internal PCollection.
// 4. Check that all transforms are in the same environment or are environment agnostic. (TODO for xlang)
// 5. Validate that only the primary input consuming transform are stateful. (Might be able to relax this)
//
// Those final steps are necessary to validate that the stage doesn't have any issues, WRT retries or similar.
//
// A PCollection produced by a transform in this stage is in the output set if it's consumed by a transform outside of the stage.
//
// Finally, it takes this information and caches it in the stage for simpler descriptor construction downstream.
//
// Note, this is very similar to the work done WRT composites in pipelinex.Normalize.
func prepareStage(stg *stage, comps *pipepb.Components, pipelineConsumers map[string][]link) {
	// Collect all PCollections involved in this stage.
	pcolParents, pcolConsumers := computPColFacts(stg.transforms, comps)

	transformSet := map[string]bool{}
	for _, tid := range stg.transforms {
		transformSet[tid] = true
	}

	// Now we can see which consumers (inputs) aren't covered by the parents (outputs).
	mainInputs := map[string]string{}
	var sideInputs []link
	inputs := map[string]bool{}
	for pid, plinks := range pcolConsumers {
		// Check if this PCollection is generated in this bundle.
		if _, ok := pcolParents[pid]; ok {
			// It is, so we will ignore for now.
			continue
		}
		// Add this collection to our input set.
		inputs[pid] = true
		for _, link := range plinks {
			t := comps.GetTransforms()[link.transform]
			sis, _ := getSideInputs(t)
			if _, ok := sis[link.local]; ok {
				sideInputs = append(sideInputs, link)
			} else {
				mainInputs[link.global] = link.global
			}
		}
	}
	outputs := map[string]link{}
	var internal []string
	// Look at all PCollections produced in this stage.
	for pid, link := range pcolParents {
		// Look at all consumers of this PCollection in the pipeline
		isInternal := true
		for _, l := range pipelineConsumers[pid] {
			// If the consuming transform isn't in the stage, it's an output.
			if !transformSet[l.transform] {
				isInternal = false
				outputs[pid] = link
			}
		}
		// It's consumed as an output, we already ensure the coder's in the set.
		if isInternal {
			internal = append(internal, pid)
		}
	}

	stg.internalCols = internal
	stg.outputs = maps.Values(outputs)
	stg.sideInputs = sideInputs

	defer func() {
		if e := recover(); e != nil {
			panic(fmt.Sprintf("stage %+v:\n%v\n\n%v", stg, e, prototext.Format(comps)))
		}
	}()

	// Impulses won't have any inputs.
	if l := len(mainInputs); l == 1 {
		stg.primaryInput = getOnlyValue(mainInputs)
	} else if l > 1 {
		// Quick check that this is a lone flatten node, which is handled runner side anyway
		// and only sent SDK side as part of a fused stage.
		if !(len(stg.transforms) == 1 && comps.GetTransforms()[stg.transforms[0]].GetSpec().GetUrn() == urns.TransformFlatten) {
			panic("expected flatten node, but wasn't")
		}
	}
}
