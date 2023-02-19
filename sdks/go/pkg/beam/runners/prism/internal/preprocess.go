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
	"sort"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/pipelinex"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slog"
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
	slog.Debug("topological transform ordering", topological)

	var stages []*stage
	for _, tid := range topological {
		stages = append(stages, &stage{
			transforms: []string{tid},
		})
	}
	return stages
}
