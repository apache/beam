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

package graph

import (
	"testing"
)

// TestBuildValid tests that Build succeeds in a valid graph.
func TestBuildValid(t *testing.T) {
	g := New()
	NewImpulse(g, g.Root(), []byte{})
	if _, _, err := g.Build(); err != nil {
		t.Errorf("g.Build() = %v, want: nil", err)
	}
}

// TestBuildNoCoder tests that Build fails when a node has no defined coder.
func TestBuildNoCoder(t *testing.T) {
	g := New()
	NewImpulse(g, g.Root(), []byte{})
	// Sets node's coder to nil
	g.nodes[0].Coder = nil

	if _, _, err := g.Build(); err == nil {
		t.Errorf("g.Build() = nil, want: undefined coder")
	}
}

// TestBuildUnconnectedNode tests that Build fails when a node in the graph is unconnected.
func TestBuildUnconnectedNode(t *testing.T) {
	g := New()
	edge := NewImpulse(g, g.Root(), []byte{})
	// Disconnects node from edge.
	edge.Output = []*Outbound{}

	if _, _, err := g.Build(); err == nil {
		t.Errorf("g.Build() = nil, want: unconnected node")
	}
}

// TestBuildNodeNotInGraph tests that Build fails when edge in graph h connects to node in graph g.
func TestBuildNodeNotInGraph(t *testing.T) {
	g := New()
	h := New()

	NewImpulse(g, g.Root(), []byte{})
	edge := NewImpulse(h, h.Root(), []byte{})
	// Adds node from graph g to output of an edge of h.
	edge.Output = append(edge.Output, &Outbound{To: g.nodes[0]})

	if _, _, err := h.Build(); err == nil {
		t.Errorf("g.Build() = nil, want: node not in graph")
	}
}
