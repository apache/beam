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
	"reflect"

	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"
)

// This file retains the logic for the pardo handler

// ParDoCharacteristic holds the configuration for ParDos.
type ParDoCharacteristic struct {
	DisableSDF bool // Sets whether a pardo supports SDFs or not.
}

func ParDo(config any) *pardo {
	return &pardo{config: config.(ParDoCharacteristic)}
}

// pardo represents an instance of the pardo handler.
type pardo struct {
	config ParDoCharacteristic
}

// ConfigURN returns the name for combine in the configuration file.
func (*pardo) ConfigURN() string {
	return "pardo"
}

func (*pardo) ConfigCharacteristic() reflect.Type {
	return reflect.TypeOf((*ParDoCharacteristic)(nil)).Elem()
}

var _ transformPreparer = (*pardo)(nil)

func (*pardo) PrepareUrns() []string {
	return []string{urns.TransformParDo}
}

// PrepareTransform handles special processing with respect to ParDos, since their handling is dependant on supported features
// and requirements.
func (h *pardo) PrepareTransform(tid string, t *pipepb.PTransform, comps *pipepb.Components) prepareResult {

	// ParDos are a pain in the butt.
	// Combines, by comparison, are dramatically simpler.
	// This is because for ParDos, how they are handled, and what kinds of transforms are in
	// and around the ParDo, the actual shape of the graph will change.
	// At their simplest, it's something a DoFn will handle on their own.
	// At their most complex, they require intimate interaction with the subgraph
	// bundling process, the data layer, state layers, and control layers.
	// But unlike combines, which have a clear urn for composite + special payload,
	// ParDos have the standard URN for composites with the standard payload.
	// So always, we need to first unmarshal the payload.

	pardoPayload := t.GetSpec().GetPayload()
	pdo := &pipepb.ParDoPayload{}
	if err := (proto.UnmarshalOptions{}).Unmarshal(pardoPayload, pdo); err != nil {
		panic(fmt.Sprintf("unable to decode ParDoPayload for transform[%v]", t.GetUniqueName()))
	}

	// Lets check for and remove anything that makes things less simple.
	if pdo.RestrictionCoderId == "" {
		// Which inputs are Side inputs don't change the graph further,
		// so they're not included here. Any nearly any ParDo can have them.

		// At their simplest, we don't need to do anything special at pre-processing time, and simply pass through as normal.

		// StatefulDoFns need to be marked as being roots.
		var forcedRoots []string
		if len(pdo.StateSpecs)+len(pdo.TimerFamilySpecs) > 0 {
			forcedRoots = append(forcedRoots, tid)
		}

		return prepareResult{
			SubbedComps: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					tid: t,
				},
			},
			ForcedRoots: forcedRoots,
		}
	}

	// Side inputs add to topology and make fusion harder to deal with
	// (side input producers can't be in the same stage as their consumers)

	// State, Timers, Stable Input, Time Sorted Input, and some parts of SDF
	// Are easier to deal with by including a fusion break. But we can do that with a
	// runner specific transform for stable input, and another for time sorted input.
	// TODO add

	// SplittableDoFns have 3 required phases and a 4th optional phase.
	//
	// PAIR_WITH_RESTRICTION which pairs elements with their restrictions
	// Input: element;   := INPUT
	// Output: KV(element, restriction)  := PWR
	//
	// SPLIT_AND_SIZE_RESTRICTIONS splits the pairs into sub element ranges
	// and a relative size for each, in a float64 format.
	// Input: KV(element, restriction) := PWR
	// Output: KV(KV(element, restriction), float64)  := SPLITnSIZED
	//
	// PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS actually processes the
	// elements. This is also where splits need to be handled.
	// In particular, primary and residual splits have the same format as the input.
	// Input: KV(KV(element, restriction), size) := SPLITnSIZED
	// Output: DoFn's output.  := OUTPUT
	//
	// TRUNCATE_SIZED_RESTRICTION is how the runner has an SDK turn an
	// unbounded transform into a bound one. Not needed until the pipeline
	// is told to drain.
	// Input: KV(KV(element, restriction), float64) := synthetic split results from above
	// Output: KV(KV(element, restriction), float64). := synthetic, truncated results sent as Split n Sized
	//
	// So with that, we can figure out the coders we need.
	//
	// cE - Element Coder  (same as input coder)
	// cR - Restriction Coder
	// cS - Size Coder (float64)
	// ckvER - KV<Element, Restriction>
	// ckvERS - KV<KV<Element, Restriction>, Size>
	//
	// There could be a few output coders, but the outputs can be copied from
	// the original transform directly.

	// First lets get the parallel input coder ID.
	var pcolInID, inputLocalID string
	for localID, globalID := range t.GetInputs() {
		// The parallel input is the one that isn't a side input.
		if _, ok := pdo.SideInputs[localID]; !ok {
			inputLocalID = localID
			pcolInID = globalID
			break
		}
	}
	inputPCol := comps.GetPcollections()[pcolInID]
	cEID := inputPCol.GetCoderId()
	cRID := pdo.RestrictionCoderId
	cSID := "c" + tid + "size"
	ckvERID := "c" + tid + "kv_ele_rest"
	ckvERSID := ckvERID + "_size"

	coder := func(urn string, componentIDs ...string) *pipepb.Coder {
		return &pipepb.Coder{
			Spec: &pipepb.FunctionSpec{
				Urn: urn,
			},
			ComponentCoderIds: componentIDs,
		}
	}

	coders := map[string]*pipepb.Coder{
		ckvERID:  coder(urns.CoderKV, cEID, cRID),
		cSID:     coder(urns.CoderDouble),
		ckvERSID: coder(urns.CoderKV, ckvERID, cSID),
	}

	// There are only two new PCollections.
	// INPUT -> same as ordinary DoFn
	// PWR, uses ckvER
	// SPLITnSIZED, uses ckvERS
	// OUTPUT -> same as ordinary outputs

	nPWRID := "n" + tid + "_pwr"
	nSPLITnSIZEDID := "n" + tid + "_splitnsized"

	pcol := func(name, coderID string) *pipepb.PCollection {
		return &pipepb.PCollection{
			UniqueName:          name,
			CoderId:             coderID,
			IsBounded:           inputPCol.GetIsBounded(),
			WindowingStrategyId: inputPCol.GetWindowingStrategyId(),
		}
	}

	pcols := map[string]*pipepb.PCollection{
		nPWRID:         pcol(nPWRID, ckvERID),
		nSPLITnSIZEDID: pcol(nSPLITnSIZEDID, ckvERSID),
	}

	// There are 3 new PTransforms, with process sized elements and restrictions
	// taking the brunt of the complexity, consuming the inputs

	ePWRID := "e" + tid + "_pwr"
	eSPLITnSIZEDID := "e" + tid + "_splitnsize"
	eProcessID := "e" + tid + "_processandsplit"

	tform := func(name, urn, in, out string) *pipepb.PTransform {
		// Apparently we also send side inputs to PairWithRestriction
		// and SplitAndSize. We should consider wether we could simply
		// drop the side inputs from the ParDo payload instead, which
		// could lead to an additional fusion oppportunity.
		newInputs := maps.Clone(t.GetInputs())
		newInputs[inputLocalID] = in
		return &pipepb.PTransform{
			UniqueName: name,
			Spec: &pipepb.FunctionSpec{
				Urn:     urn,
				Payload: pardoPayload,
			},
			Inputs: newInputs,
			Outputs: map[string]string{
				"i0": out,
			},
			EnvironmentId: t.GetEnvironmentId(),
		}
	}

	newInputs := maps.Clone(t.GetInputs())
	newInputs[inputLocalID] = nSPLITnSIZEDID

	tforms := map[string]*pipepb.PTransform{
		ePWRID:         tform(ePWRID, urns.TransformPairWithRestriction, pcolInID, nPWRID),
		eSPLITnSIZEDID: tform(eSPLITnSIZEDID, urns.TransformSplitAndSize, nPWRID, nSPLITnSIZEDID),
		eProcessID: {
			UniqueName: eProcessID,
			Spec: &pipepb.FunctionSpec{
				Urn:     urns.TransformProcessSizedElements,
				Payload: pardoPayload,
			},
			Inputs:        newInputs,
			Outputs:       t.GetOutputs(),
			EnvironmentId: t.GetEnvironmentId(),
		},
	}
	return prepareResult{
		SubbedComps: &pipepb.Components{
			Coders:       coders,
			Pcollections: pcols,
			Transforms:   tforms,
		},
		RemovedLeaves: removeSubTransforms(comps, t.GetSubtransforms()),
		// Force ProcessSized to be a root to ensure SDFs are able to split
		// between elements or within elements.
		// Also this is where a transform would be stateful anyway.
		ForcedRoots: []string{eProcessID},
	}
}
