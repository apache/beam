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
	"google.golang.org/protobuf/proto"
)

// This file retains the logic for the combine handler

// CombineCharacteristic holds the configuration for Combines.
type CombineCharacteristic struct {
	EnableLifting bool // Sets whether a combine composite does combiner lifting or not.
}

// TODO figure out the factory we'd like.

func Combine(config any) *combine {
	return &combine{config: config.(CombineCharacteristic)}
}

// combine represents an instance of the combine handler.
type combine struct {
	config CombineCharacteristic
}

// ConfigURN returns the name for combine in the configuration file.
func (*combine) ConfigURN() string {
	return "combine"
}

func (*combine) ConfigCharacteristic() reflect.Type {
	return reflect.TypeOf((*CombineCharacteristic)(nil)).Elem()
}

var _ transformPreparer = (*combine)(nil)

func (*combine) PrepareUrns() []string {
	return []string{urns.TransformCombinePerKey}
}

// PrepareTransform returns lifted combines and removes the leaves if enabled. Otherwise returns nothing.
func (h *combine) PrepareTransform(tid string, t *pipepb.PTransform, comps *pipepb.Components) prepareResult {
	// If we aren't lifting, the "default impl" for combines should be sufficient.
	if !h.config.EnableLifting {
		return prepareResult{
			SubbedComps: &pipepb.Components{
				Transforms: map[string]*pipepb.PTransform{
					tid: t,
				},
			},
		}
	}

	// To lift a combine, the spec should contain a CombinePayload.
	// That contains the actual FunctionSpec for the DoFn, and the
	// id for the accumulator coder.
	// We can synthetically produce/determine the remaining coders for
	// the Input and Output types from the existing PCollections.
	//
	// This means we also need to synthesize pcollections with the accumulator coder too.

	// What we have:
	//  Input PCol: KV<K, I>      -- INPUT
	//  -> GBK := KV<K, Iter<I>>  -- GROUPED_I
	//  -> Combine := KV<K, O>    -- OUTPUT
	//
	// What we want:
	//  Input PCol: KV<K, I>             -- INPUT
	//  -> PreCombine := KV<K, A>        -- LIFTED
	//  -> GBK -> KV<K, Iter<A>>         -- GROUPED_A
	//  -> MergeAccumulators := KV<K, A> -- MERGED_A
	//  -> ExtractOutput -> KV<K, O>     -- OUTPUT
	//
	// First we need to produce new coders for Iter<A>, KV<K, Iter<A>>, and KV<K, A>.
	// The A coder ID is in the combine payload.
	//
	// Then we can produce the PCollections.
	// We can reuse the INPUT and OUTPUT PCollections.
	// We need LIFTED to have KV<K, A>  kv_k_a
	// We need GROUPED_A to have KV<K, Iter<A>> kv_k_iter_a
	// We need MERGED_A to have KV<K, A> kv_k_a
	//
	// GROUPED_I ends up unused.
	//
	// The PCollections inherit the properties of the Input PCollection
	// such as Boundedness, and Windowing Strategy.
	//
	// With these, we can produce the PTransforms with the appropriate URNs for the
	// different parts of the composite, and return the new components.

	cmbPayload := t.GetSpec().GetPayload()
	cmb := &pipepb.CombinePayload{}
	if err := (proto.UnmarshalOptions{}).Unmarshal(cmbPayload, cmb); err != nil {
		panic(fmt.Sprintf("unable to decode ParDoPayload for transform[%v]", t.GetUniqueName()))
	}

	// First lets get the key coder ID.
	var pcolInID string
	// There's only one input.
	for _, pcol := range t.GetInputs() {
		pcolInID = pcol
	}
	inputPCol := comps.GetPcollections()[pcolInID]
	kvkiID := inputPCol.GetCoderId()
	kID := comps.GetCoders()[kvkiID].GetComponentCoderIds()[0]

	// Now we can start synthesis!
	// Coder IDs
	aID := cmb.AccumulatorCoderId

	ckvprefix := "c" + tid + "_kv_"

	iterACID := "c" + tid + "_iter_" + aID
	kvkaCID := ckvprefix + kID + "_" + aID
	kvkIterACID := ckvprefix + kID + "_iter" + aID

	// PCollection IDs
	nprefix := "n" + tid + "_"
	liftedNID := nprefix + "lifted"
	groupedNID := nprefix + "grouped"
	mergedNID := nprefix + "merged"

	// Now we need the output collection ID
	var pcolOutID string
	// There's only one output.
	for _, pcol := range t.GetOutputs() {
		pcolOutID = pcol
	}

	// Transform IDs
	eprefix := "e" + tid + "_"
	liftEID := eprefix + "lift"
	gbkEID := eprefix + "gbk"
	mergeEID := eprefix + "merge"
	extractEID := eprefix + "extract"

	coder := func(urn string, componentIDs ...string) *pipepb.Coder {
		return &pipepb.Coder{
			Spec: &pipepb.FunctionSpec{
				Urn: urn,
			},
			ComponentCoderIds: componentIDs,
		}
	}

	pcol := func(name, coderID string) *pipepb.PCollection {
		return &pipepb.PCollection{
			UniqueName:          name,
			CoderId:             coderID,
			IsBounded:           inputPCol.GetIsBounded(),
			WindowingStrategyId: inputPCol.GetWindowingStrategyId(),
		}
	}

	tform := func(name, urn, in, out, env string) *pipepb.PTransform {
		return &pipepb.PTransform{
			UniqueName: name,
			Spec: &pipepb.FunctionSpec{
				Urn:     urn,
				Payload: cmbPayload,
			},
			Inputs: map[string]string{
				"i0": in,
			},
			Outputs: map[string]string{
				"i0": out,
			},
			EnvironmentId: env,
		}
	}

	newComps := &pipepb.Components{
		Coders: map[string]*pipepb.Coder{
			iterACID:    coder(urns.CoderIterable, aID),
			kvkaCID:     coder(urns.CoderKV, kID, aID),
			kvkIterACID: coder(urns.CoderKV, kID, iterACID),
		},
		Pcollections: map[string]*pipepb.PCollection{
			liftedNID:  pcol(liftedNID, kvkaCID),
			groupedNID: pcol(groupedNID, kvkIterACID),
			mergedNID:  pcol(mergedNID, kvkaCID),
		},
		Transforms: map[string]*pipepb.PTransform{
			liftEID:    tform(liftEID, urns.TransformPreCombine, pcolInID, liftedNID, t.GetEnvironmentId()),
			gbkEID:     tform(gbkEID, urns.TransformGBK, liftedNID, groupedNID, ""),
			mergeEID:   tform(mergeEID, urns.TransformMerge, groupedNID, mergedNID, t.GetEnvironmentId()),
			extractEID: tform(extractEID, urns.TransformExtract, mergedNID, pcolOutID, t.GetEnvironmentId()),
		},
	}

	// We don't need to remove the composite, since we don't add it in
	// when we return the new transforms, so it's not in the topology.
	return prepareResult{
		SubbedComps:   newComps,
		RemovedLeaves: removeSubTransforms(comps, t.GetSubtransforms()),
	}
}
