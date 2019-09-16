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

import pb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
import "github.com/golang/protobuf/proto"

// Bounded returns true iff all PCollections are bounded.
func Bounded(p *pb.Pipeline) bool {
	for _, col := range p.GetComponents().GetPcollections() {
		if col.IsBounded == pb.IsBounded_UNBOUNDED {
			return false
		}
	}
	return true
}

// ContainerImages returns the set of container images used
// in the given pipeline.
func ContainerImages(p *pb.Pipeline) []string {
	var ret []string
	for _, t := range p.GetComponents().GetEnvironments() {
		// TODO(angoenka) 09/14/2018 Check t.Urn before parsing the payload.
		var payload pb.DockerPayload
		proto.Unmarshal(t.GetPayload(), &payload)
		ret = append(ret, payload.ContainerImage)
	}
	return ret
}

// TopologicalSort returns a topologically sorted list of the given
// ids, generally from the same scope/composite. Assumes acyclic graph.
func TopologicalSort(xforms map[string]*pb.PTransform, ids []string) []string {
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

func newVisiter(xforms map[string]*pb.PTransform, ids []string) *visiter {
	ret := &visiter{
		output: make([]string, len(ids), len(ids)),
		index:  len(ids) - 1,
		seen:   make(map[string]bool),
		next:   make(map[string][]string),
	}
	for _, id := range ids {
		for _, in := range xforms[id].Inputs {
			ret.next[in] = append(ret.next[in], id)
		}
	}
	return ret
}

func (v *visiter) visit(xforms map[string]*pb.PTransform, id string) {
	if v.seen[id] {
		return
	}
	v.seen[id] = true
	for _, out := range xforms[id].Outputs {
		for _, next := range v.next[out] {
			v.visit(xforms, next)
		}
	}

	v.output[v.index] = id
	v.index--
}
