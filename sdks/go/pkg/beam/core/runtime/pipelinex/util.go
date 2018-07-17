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
		ret = append(ret, t.Url)
	}
	return ret
}
