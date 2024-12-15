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
	"log/slog"
	"sort"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/pipelinex"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/engine"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/jobservices"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"
)

// transformPreparer is an interface for handling different urns in the preprocessor
// largely for exchanging transforms for others, to be added to the complete set of
// components in the pipeline.
type transformPreparer interface {
	// PrepareUrns returns the Beam URNs that this handler deals with for preprocessing.
	PrepareUrns() []string
	// PrepareTransform takes a PTransform proto and returns a set of new Components, and a list of
	// transformIDs leaves to remove and ignore from graph processing.
	PrepareTransform(tid string, t *pipepb.PTransform, comps *pipepb.Components) prepareResult
}

type prepareResult struct {
	SubbedComps   *pipepb.Components
	RemovedLeaves []string
	ForcedRoots   []string
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
func (p *preprocessor) preProcessGraph(comps *pipepb.Components, j *jobservices.Job) []*stage {
	ts := comps.GetTransforms()

	// TODO move this out of this part of the pre-processor?
	leaves := map[string]struct{}{}
	ignore := map[string]struct{}{}
	forcedRoots := map[string]bool{}
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
			}
			continue
		}

		prepResult := h.PrepareTransform(tid, t, comps)

		// Clear out unnecessary leaves from this composite for topological sort handling.
		for _, key := range prepResult.RemovedLeaves {
			ignore[key] = struct{}{}
			delete(leaves, key)
		}
		for _, key := range prepResult.ForcedRoots {
			forcedRoots[key] = true
		}

		// ts should be a clone, so we should be able to add new transforms into the map.
		for tid, t := range prepResult.SubbedComps.GetTransforms() {
			leaves[tid] = struct{}{}
			ts[tid] = t
		}
		for cid, c := range prepResult.SubbedComps.GetCoders() {
			comps.GetCoders()[cid] = c
		}
		for nid, n := range prepResult.SubbedComps.GetPcollections() {
			comps.GetPcollections()[nid] = n
		}
		// It's unlikely for these to change, but better to handle them now, to save a headache later.
		for wid, w := range prepResult.SubbedComps.GetWindowingStrategies() {
			comps.GetWindowingStrategies()[wid] = w
		}
		for envid, env := range prepResult.SubbedComps.GetEnvironments() {
			comps.GetEnvironments()[envid] = env
		}
	}

	// Extract URNs for the given transform.

	keptLeaves := maps.Keys(leaves)
	sort.Strings(keptLeaves)
	topological := pipelinex.TopologicalSort(ts, keptLeaves)
	slog.Debug("topological transform ordering", slog.Any("topological", topological))

	facts, err := computeFacts(topological, comps)
	if err != nil {
		err = fmt.Errorf("error computing pipeline facts: %w", err)
		j.SendMsg(err.Error())
		j.Failed(err)
		return nil
	}
	facts.ForcedRoots = forcedRoots

	// avoid "unused" warnings while keeping the older default approach available.
	_ = greedyFusion
	_ = defaultFusion

	stages := greedyFusion(topological, comps, facts)

	for i, stg := range stages {
		err := finalizeStage(stg, comps, facts)
		if err != nil {
			err = fmt.Errorf("preprocess validation failure of stage %v: %v", i, err)
			j.SendMsg(err.Error())
			j.Failed(err)
			return nil
		}
	}
	return stages
}

// removeSubTransforms recurses over the set of transforms and removes all sub transforms
// as well, as they should no longer be processed.
//
// This is a helper method for the handlers.
func removeSubTransforms(comps *pipepb.Components, toRemove []string) []string {
	var removals []string
	for _, stid := range toRemove {
		removals = append(removals, stid)
		removals = append(removals, removeSubTransforms(comps, comps.GetTransforms()[stid].GetSubtransforms())...)
	}
	return removals
}

// TODO(lostluck): Be able to toggle this in variants.
// Most likely, re-implement in terms of simply marking all transforms as forced roots.
// Commented out to avoid the unused staticheck, but it's worth keeping until the docs
// and implementation is re-added.

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
func defaultFusion(topological []string, comps *pipepb.Components, facts fusionFacts) []*stage {
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

		producerLink := facts.PcolProducers[inputID]

		producer := comps.GetTransforms()[producerLink.Transform]

		// Check if the input source is a GBK
		if producer.GetSpec().GetUrn() != urns.TransformGBK {
			continue
		}

		// Check if the coder is a KV<K, Iter<?>>
		iCID := comps.GetPcollections()[inputID].GetCoderId()
		oCID := comps.GetPcollections()[outputID].GetCoderId()

		if checkForExpandCoderPattern(iCID, oCID, comps) {
			fuseWithConsumers[tid] = outputID
		}
	}

	var stages []*stage
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
		cs := facts.PcolConsumers[pcolID]

		for _, c := range cs {
			stg.transforms = append(stg.transforms, c.Transform)
			consumed[c.Transform] = true
		}
	}
	return stages
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

type fusionFacts struct {
	PcolProducers   map[string]link   // global pcol ID to transform link that produces it.
	PcolConsumers   map[string][]link // global pcol ID to all consumers of that pcollection
	UsedAsSideInput map[string]bool   // global pcol ID and if it's used as a side input

	DirectSideInputs     map[string]map[string]bool // global transform ID and all direct side input pcollections.
	DownstreamSideInputs map[string]map[string]bool // global transform ID and all transitive side input pcollections.

	ForcedRoots map[string]bool // transforms forced to be roots (not computed in computeFacts)
}

// computeFacts computes facts about the given set of transforms and components that
// are useful for fusion.
func computeFacts(topological []string, comps *pipepb.Components) (*fusionFacts, error) {
	ret := &fusionFacts{
		PcolProducers:        map[string]link{},
		PcolConsumers:        map[string][]link{},
		UsedAsSideInput:      map[string]bool{},
		DirectSideInputs:     map[string]map[string]bool{}, // direct set
		DownstreamSideInputs: map[string]map[string]bool{}, // transitive set
	}

	// Use the topological ids so each PCollection only has a single
	// producer. We've already pruned out composites at this stage.
	for _, tID := range topological {
		t := comps.GetTransforms()[tID]
		for local, global := range t.GetOutputs() {
			if p, ok := ret.PcolProducers[global]; ok {
				return nil, fmt.Errorf("computeFacts: two producers for one PCollection: %v and %v", p, link{Transform: tID, Local: local, Global: global})
			}
			ret.PcolProducers[global] = link{Transform: tID, Local: local, Global: global}
		}
		sis, err := getSideInputs(t)
		if err != nil {
			return nil, fmt.Errorf("computeFacts: unable to check %q side inputs", tID)
		}
		directSIs := map[string]bool{}
		ret.DirectSideInputs[tID] = directSIs
		for local, global := range t.GetInputs() {
			ret.PcolConsumers[global] = append(ret.PcolConsumers[global], link{Transform: tID, Local: local, Global: global})
			if _, ok := sis[local]; ok {
				ret.UsedAsSideInput[global] = true
				directSIs[global] = true
			}
		}
	}

	for _, tID := range topological {
		computeDownstreamSideInputs(tID, comps, ret)
	}

	return ret, nil
}

func computeDownstreamSideInputs(tID string, comps *pipepb.Components, facts *fusionFacts) map[string]bool {
	if dssi, ok := facts.DownstreamSideInputs[tID]; ok {
		return dssi
	}
	dssi := map[string]bool{}
	for _, o := range comps.GetTransforms()[tID].GetOutputs() {
		if facts.UsedAsSideInput[o] {
			dssi[o] = true
		}
		for _, consumer := range facts.PcolConsumers[o] {
			cdssi := computeDownstreamSideInputs(consumer.Transform, comps, facts)
			maps.Copy(dssi, cdssi)
		}
	}
	facts.DownstreamSideInputs[tID] = dssi
	return dssi
}

// finalizeStage does the final pre-processing step for stages:
//
// 1. Determining the single parallel input (may be 0 for impulse stages).
// 2. Determining all outputs to the stages.
// 3. Determining all side inputs.
// 4  Validating that no side input is fed by an internal PCollection.
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
func finalizeStage(stg *stage, comps *pipepb.Components, pipelineFacts *fusionFacts) error {
	// Collect all PCollections involved in this stage.
	stageFacts, err := computeFacts(stg.transforms, comps)
	if err != nil {
		return err
	}

	transformSet := map[string]bool{}
	for _, tid := range stg.transforms {
		transformSet[tid] = true
	}

	// Now we can see which consumers (inputs) aren't covered by the producers (outputs).
	mainInputs := map[string]string{}
	var sideInputs []engine.LinkID
	inputs := map[string]bool{}
	for pid, plinks := range stageFacts.PcolConsumers {
		// Check if this PCollection is generated in this bundle.
		if _, ok := stageFacts.PcolProducers[pid]; ok {
			// It is, so we will ignore for now.
			continue
		}
		// Add this collection to our input set.
		inputs[pid] = true
		for _, link := range plinks {
			t := comps.GetTransforms()[link.Transform]

			var sis map[string]*pipepb.SideInput
			switch t.GetSpec().GetUrn() {
			case urns.TransformParDo, urns.TransformProcessSizedElements, urns.TransformPairWithRestriction, urns.TransformSplitAndSize, urns.TransformTruncate:
				pardo := &pipepb.ParDoPayload{}
				if err := (proto.UnmarshalOptions{}).Unmarshal(t.GetSpec().GetPayload(), pardo); err != nil {
					return fmt.Errorf("unable to decode ParDoPayload for %v", link.Transform)
				}
				stg.finalize = pardo.RequestsFinalization
				if len(pardo.GetTimerFamilySpecs())+len(pardo.GetStateSpecs())+len(pardo.GetOnWindowExpirationTimerFamilySpec()) > 0 {
					stg.stateful = true
				}
				if pardo.GetOnWindowExpirationTimerFamilySpec() != "" {
					stg.onWindowExpiration = engine.StaticTimerID{TransformID: link.Transform, TimerFamily: pardo.GetOnWindowExpirationTimerFamilySpec()}
				}
				sis = pardo.GetSideInputs()
			}
			if _, ok := sis[link.Local]; ok {
				sideInputs = append(sideInputs, engine.LinkID{Transform: link.Transform, Global: link.Global, Local: link.Local})
			} else {
				mainInputs[link.Global] = link.Global
			}
		}
	}
	outputs := map[string]link{}
	var internal []string
	// Look at all PCollections produced in this stage.
	for pid, link := range stageFacts.PcolProducers {
		// Look at all consumers of this PCollection in the pipeline
		isInternal := true
		for _, l := range pipelineFacts.PcolConsumers[pid] {
			// If the consuming transform isn't in the stage, it's an output.
			if !transformSet[l.Transform] {
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

	// Impulses won't have any inputs.
	if l := len(mainInputs); l == 1 {
		stg.primaryInput = getOnlyValue(mainInputs)
	} else if l > 1 {
		// Quick check that this is lead by a flatten node, and that it's handled runner side.
		t := comps.GetTransforms()[stg.transforms[0]]
		if !(t.GetSpec().GetUrn() == urns.TransformFlatten && t.GetEnvironmentId() == "") {
			formatMap := func(in map[string]string) string {
				var b strings.Builder
				for k, v := range in {
					b.WriteString(k)
					b.WriteString(" : ")
					b.WriteString(v)
					b.WriteString("\n\t")
				}
				return b.String()
			}
			return fmt.Errorf("stage requires multiple parallel inputs but wasn't a flatten:\n\ttransforms\n\t%v\n\tmain inputs\n\t%v\n\tsidinputs\n\t%v", strings.Join(stg.transforms, "\n\t\t"), formatMap(mainInputs), sideInputs)
		}
	}
	return nil
}

// greedyFusion produces a pipeline as tightly fused as possible.
//
// Fusion is a critical optimization for performance of pipeline execution.
// Thus it's important for SDKs to be capable of executing transforms in a fused state.
//
// However, not all transforms can be fused into the same stage together.
// Further, some transforms must be at the root of a stage.
//
// # Fusion Restrictions
//
// Environments: Transforms that aren't in the same environment can't be
// fused together *unless* their environments can also be fused together.
// Eg. Resource hints can often be ignored for local runners.
//
// Side Inputs: A transform S consuming a PCollection as a side input can't
// be fused with the  transform P that produces that PCollection. Further,
// no transform S+ descended from S, can be fused with transform P.
//
// Splittable DoFns: An expanded Splittable DoFn transform's Process Sized
// Elements and Restrictions component must be the root of a stage.
//
// State and Timers: Stateful Transforms (transforms using State and Timers)
// must be the root of transforms, since they are required to be keyed.
// A sequence of Key Preserving stateful transforms could be fused.
//
// TODO: Sink/Unzip Flattens so they vanish from the graph.
//
// This approach is largely cribed from the Python approach at
// fn_api_runner/translations.py. That implementation is very set oriented &
// eagerly adds data source/sink transforms, while prism does so later in
// stage construction.
func greedyFusion(topological []string, comps *pipepb.Components, facts *fusionFacts) []*stage {
	fused := map[int]int{}
	stageAssignments := map[string]int{}

	stageEnvs := map[int]string{}
	forcedRoots := map[int]bool{}
	directSIs := map[int]map[string]bool{}
	downstreamSIs := map[int]map[string]bool{}

	var index int
	replacements := func(tID string) int {
		sID, ok := stageAssignments[tID]
		if !ok { // No stage exists yet.
			sID = index
			index++

			t := comps.GetTransforms()[tID]
			stageAssignments[tID] = sID
			stageEnvs[sID] = t.GetEnvironmentId()
			forcedRoots[sID] = facts.ForcedRoots[tID]
			directSIs[sID] = maps.Clone(facts.DirectSideInputs[tID])
			downstreamSIs[sID] = maps.Clone(facts.DownstreamSideInputs[tID])
		}

		var oldIDs []int
		rep, ok := fused[sID]
		for ok {
			oldIDs = append(oldIDs, sID)
			sID = rep
			rep, ok = fused[sID]
		}
		// Update the assignment & fusions for path shortening.
		stageAssignments[tID] = sID
		for _, old := range oldIDs {
			fused[old] = sID
		}
		return sID
	}

	overlap := func(downstream, consumer map[string]bool) bool {
		for si := range consumer {
			if downstream[si] {
				return true
			}
		}
		return false
	}

	// To start, every transform is in it's own stage.
	// So we map a transformID to a stageID.
	// We go through each PCollection, (facts.PcolProducers) and
	// try to fuse the producer to each consumer of that PCollection.
	//
	// If we can fuse, the consumer takes on the producer's stageID,
	// and the assignments are updated.

	var topoPcols []string
	for _, tid := range topological {
		for _, global := range comps.GetTransforms()[tid].GetOutputs() {
			topoPcols = append(topoPcols, global)
		}
	}
	for _, pcol := range topoPcols {
		producer := facts.PcolProducers[pcol]
		for _, consumer := range facts.PcolConsumers[pcol] {
			pID := replacements(producer.Transform) // Get current stage for producer
			cID := replacements(consumer.Transform) // Get current stage for consumer

			// See if there's anything preventing fusion:
			if pID == cID {
				continue // Already fused together.
			}
			if stageEnvs[pID] != stageEnvs[cID] {
				continue // Not the same environment.
			}
			if forcedRoots[cID] {
				continue // Forced root.
			}
			if overlap(downstreamSIs[pID], directSIs[cID]) {
				continue // Side input conflict
			}

			// In principle, we can fuse!
			fused[cID] = pID // Set the consumer to be in the producer's stage.
			// Copy the consumer's direct and downstream side input sets into the producer.
			maps.Copy(directSIs[pID], directSIs[cID])
			maps.Copy(downstreamSIs[pID], downstreamSIs[cID])
		}
	}

	var stages []*stage
	fusedToStages := map[int]*stage{}
	for _, tID := range topological {
		sID := replacements(tID)
		s := fusedToStages[sID]
		if s == nil {
			s = &stage{
				envID: stageEnvs[sID],
			}
			fusedToStages[sID] = s
			stages = append(stages, s)
		}
		s.transforms = append(s.transforms, tID)
	}
	return stages
}
