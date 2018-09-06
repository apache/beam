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

package graphx

import (
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
)

// NamedEdge is a named MultiEdge.
type NamedEdge struct {
	Name string
	Edge *graph.MultiEdge
}

// NamedScope is a named Scope.
type NamedScope struct {
	Name  string
	Scope *graph.Scope
}

// ScopeTree is a convenient representation of the Scope-structure as a tree.
// Each ScopeTree may also be a subtree of another ScopeTree. The tree structure
// respects the underlying Scope structure, i.e., if Scope 'a' has a parent
// 'b' then the ScopeTree for 'b' must have the ScopeTree for 'a' as a child.
type ScopeTree struct {
	// Scope is the named scope at the root of the (sub)tree.
	Scope NamedScope
	// Edges are the named edges directly under this scope.
	Edges []NamedEdge

	// Children are the scopes directly under this scope.
	Children []*ScopeTree
}

// NewScopeTree computes the ScopeTree for a set of edges.
func NewScopeTree(edges []*graph.MultiEdge) *ScopeTree {
	t := newTreeBuilder()
	for _, edge := range edges {
		t.addEdge(edge)
	}
	return t.root
}

// treeBuilder is a builder of a ScopeTree from any set of edges and
// scopes from the same graph.
type treeBuilder struct {
	root    *ScopeTree
	id2tree map[int]*ScopeTree
}

func newTreeBuilder() *treeBuilder {
	return &treeBuilder{
		id2tree: make(map[int]*ScopeTree),
	}
}

func (t *treeBuilder) addEdge(edge *graph.MultiEdge) {
	tree := t.addScope(edge.Scope())
	tree.Edges = append(tree.Edges, NamedEdge{Name: edge.Name(), Edge: edge})
}

func (t *treeBuilder) addScope(s *graph.Scope) *ScopeTree {
	if tree, exists := t.id2tree[s.ID()]; exists {
		return tree
	}

	tree := &ScopeTree{Scope: NamedScope{Name: s.Label, Scope: s}}
	t.id2tree[s.ID()] = tree

	if s.Parent == nil {
		t.root = tree
	} else {
		parent := t.addScope(s.Parent)
		parent.Children = append(parent.Children, tree)
	}
	return tree
}
