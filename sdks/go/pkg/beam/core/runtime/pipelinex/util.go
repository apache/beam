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
	"sort"

	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/protobuf/proto"
)

// Bounded returns true iff all PCollections are bounded.
func Bounded(p *pipepb.Pipeline) bool {
	for _, col := range p.GetComponents().GetPcollections() {
		if col.IsBounded == pipepb.IsBounded_UNBOUNDED {
			return false
		}
	}
	return true
}

// ContainerImages returns the set of container images used
// in the given pipeline.
//
// Deprecated: Expand manually from pipeline.environments instead.
func ContainerImages(p *pipepb.Pipeline) []string {
	var ret []string
	for _, t := range p.GetComponents().GetEnvironments() {
		var payload pipepb.DockerPayload
		proto.Unmarshal(t.GetPayload(), &payload)
		ret = append(ret, payload.ContainerImage)
	}
	return ret
}

// TopologicalSort returns a topologically sorted list of the given
// ids, generally from the same scope/composite. Assumes acyclic graph.
func TopologicalSort(xforms map[string]*pipepb.PTransform, ids []string) []string {
	if len(ids) == 0 {
		return ids
	}

	v := newVisiter(xforms, ids)
	for _, id := range ids {
		v.visit(xforms, id)
	}
	return v.output
}

type visiter struct {
	output []string
	index  int
	seen   map[string]bool
	next   map[string][]string // collection -> transforms
}

func newVisiter(xforms map[string]*pipepb.PTransform, ids []string) *visiter {
	ret := &visiter{
		output: make([]string, len(ids)),
		index:  len(ids) - 1,
		seen:   make(map[string]bool),
		next:   make(map[string][]string),
	}
	for _, id := range ids {
		for _, in := range xforms[id].Inputs {
			ret.next[in] = append(ret.next[in], id)
		}
	}
	for _, ns := range ret.next {
		sort.Strings(ns)
	}
	return ret
}

func (v *visiter) visit(xforms map[string]*pipepb.PTransform, id string) {
	if v.seen[id] {
		return
	}
	v.seen[id] = true
	// Deterministically iterate through the output keys.
	outputKeys := make([]string, 0, len(xforms[id].Outputs))
	for _, k := range xforms[id].Outputs {
		outputKeys = append(outputKeys, k)
	}
	sort.Strings(outputKeys)

	for _, out := range outputKeys {
		for _, next := range v.next[out] {
			v.visit(xforms, next)
		}
	}
	v.output[v.index] = id
	v.index--
}

// BoolToBounded is a convenience function to get an IsBounded enum value.
func BoolToBounded(bounded bool) pipepb.IsBounded_Enum {
	if bounded {
		return pipepb.IsBounded_BOUNDED
	}
	return pipepb.IsBounded_UNBOUNDED
}
