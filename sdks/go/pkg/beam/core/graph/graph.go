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
	"fmt"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

// Graph represents an in-progress deferred execution graph and is easily
// translatable to the model graph. This graph representation allows precise
// control over scope and connectivity.
type Graph struct {
	scopes []*Scope
	edges  []*MultiEdge
	nodes  []*Node

	root *Scope
}

// New returns an empty graph with the scope set to the root.
func New() *Graph {
	root := &Scope{id: 0, Label: "root", Parent: nil}
	return &Graph{root: root}
}

// Root returns the root scope of the graph.
func (g *Graph) Root() *Scope {
	return g.root
}

// NewScope creates and returns a new scope that is a child of the supplied scope.
func (g *Graph) NewScope(parent *Scope, name string) *Scope {
	if parent == nil {
		panic("Scope is nil")
	}
	id := len(g.scopes) + 1
	s := &Scope{id: id, Label: name, Parent: parent}
	g.scopes = append(g.scopes, s)
	return s
}

// NewEdge creates a new edge of the graph in the supplied scope.
func (g *Graph) NewEdge(parent *Scope) *MultiEdge {
	if parent == nil {
		panic("Scope is nil")
	}
	id := len(g.edges) + 1
	e := &MultiEdge{id: id, parent: parent}
	g.edges = append(g.edges, e)
	return e
}

// NewNode creates a new node in the graph of the supplied fulltype.
func (g *Graph) NewNode(t typex.FullType, w *window.WindowingStrategy, bounded bool) *Node {
	if !typex.IsBound(t) {
		panic(fmt.Sprintf("Node type not bound: %v", t))
	}
	id := len(g.nodes) + 1
	n := &Node{id: id, t: t, w: w, bounded: bounded}
	g.nodes = append(g.nodes, n)
	return n
}

// Build performs finalization on the graph. It verifies the correctness of the
// graph structure, typechecks the plan and returns a slice of the edges in
// the graph.
func (g *Graph) Build() ([]*MultiEdge, []*Node, error) {
	// Build a map of all nodes listed in g.nodes.
	nodes := make(map[*Node]bool)
	for _, n := range g.nodes {
		nodes[n] = true
		if n.Coder == nil {
			return nil, nil, errors.Errorf("node %v in graph has undefined coder", n.id)
		}
	}
	// Build a map of all nodes that are reachable by g.edges.
	reachable := make(map[*Node]*MultiEdge)
	for _, e := range g.edges {
		for _, i := range e.Input {
			reachable[i.From] = e
		}
		for _, o := range e.Output {
			reachable[o.To] = e
		}
	}
	for n := range nodes {
		if _, ok := reachable[n]; !ok {
			return nil, nil, errors.Errorf("node %v in graph is unconnected", n.id)
		}
	}
	for n, e := range reachable {
		if _, ok := nodes[n]; !ok {
			return nil, nil, errors.Errorf("node %v is reachable by edge %v, but it's not in same graph", n.id, e.id)
		}
	}
	return g.edges, g.nodes, nil
}

func (g *Graph) String() string {
	var nodes []string
	for _, node := range g.nodes {
		nodes = append(nodes, node.String())
	}
	var edges []string
	for _, edge := range g.edges {
		edges = append(edges, edge.String())
	}
	return fmt.Sprintf("Nodes: %v\nEdges: %v", strings.Join(nodes, "\n"), strings.Join(edges, "\n"))
}
