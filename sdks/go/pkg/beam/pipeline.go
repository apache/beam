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

package beam

import (
	"context"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
)

// Scope is a hierarchical grouping for composite transforms. Scopes can be
// enclosed in other scopes and for a tree structure. For pipeline updates,
// the scope chain form a unique name. The scope chain can also be used for
// monitoring and visualization purposes.
type Scope struct {
	// parent is the scoped insertion point for composite transforms.
	scope *graph.Scope
	// real is the enclosing graph.
	real *graph.Graph
}

// IsValid returns true iff the Scope is valid. Any use of an invalid Scope
// will result in a panic.
func (s Scope) IsValid() bool {
	return s.real != nil && s.scope != nil
}

// Scope returns a sub-scope with the given name. The name provided may
// be augmented to ensure uniqueness.
func (s Scope) Scope(name string) Scope {
	if !s.IsValid() {
		panic("Invalid Scope")
	}
	scope := s.real.NewScope(s.scope, name)
	return Scope{scope: scope, real: s.real}
}

// WithContext creates a named subscope with an attached context for the
// represented composite transform. Values from that context may be
// extracted and added to the composite PTransform or generate a new
// environment for scoped transforms.
//
// If you're not sure whether these apply to your transform, use Scope
// instead, and do not set a context.
func (s Scope) WithContext(ctx context.Context, name string) Scope {
	newS := s.Scope(name)
	newS.scope.Context = ctx
	return newS
}

func (s Scope) String() string {
	if !s.IsValid() {
		return "<invalid>"
	}
	return s.scope.String()
}

// Pipeline manages a directed acyclic graph of primitive PTransforms, and the
// PCollections that the PTransforms consume and produce. Each Pipeline is
// self-contained and isolated from any other Pipeline. The Pipeline owns the
// PCollections and PTransforms and they can be used by that Pipeline only.
// Pipelines can safely be executed concurrently.
type Pipeline struct {
	// real is the deferred execution Graph as it is being constructed.
	real *graph.Graph
}

// NewPipeline creates a new empty pipeline.
func NewPipeline() *Pipeline {
	return &Pipeline{real: graph.New()}
}

// Root returns the root scope of the pipeline.
func (p *Pipeline) Root() Scope {
	return Scope{scope: p.real.Root(), real: p.real}
}

// TODO(herohde) 11/13/2017: consider making Build return the model Pipeline proto
// instead.

// Build validates the Pipeline and returns a lower-level representation for
// execution. It is called by runners only.
func (p *Pipeline) Build() ([]*graph.MultiEdge, []*graph.Node, error) {
	return p.real.Build()
}

func (p *Pipeline) String() string {
	return p.real.String()
}

// PipelineResult is the result of beamx.RunWithMetrics.
type PipelineResult interface {
	Metrics() metrics.Results
	JobID() string
}
