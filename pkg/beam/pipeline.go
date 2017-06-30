package beam

import (
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
)

// Pipeline manages a directed acyclic graph of primitive PTransforms, and the
// PCollections that the PTransforms consume and produce. Each Pipeline is
// self-contained and isolated from any other Pipeline. The Pipeline owns the
// PCollections and PTransforms and they can by used by that Pipeline only.
// Pipelines can safely be executed concurrently.
type Pipeline struct {
	// parent is the scoped insertion point for composite transforms.
	parent *graph.Scope
	// real is the deferred execution Graph as it is being constructed.
	real *graph.Graph
}

// NewPipeline creates a new empty pipeline.
func NewPipeline() *Pipeline {
	real := graph.New()
	return &Pipeline{real.Root(), real}
}

// Composite returns a Pipeline scoped as a composite transform. The underlying
// deferred execution Graph is the same. The scope is purely cosmetic and used
// by monitoring tools.
func (p *Pipeline) Composite(name string) *Pipeline {
	scope := p.real.NewScope(p.parent, name)
	return &Pipeline{scope, p.real}
}

// Build validates the Pipeline and returns a lower-level representation for
// execution. It is called by runners only.
func (p *Pipeline) Build() ([]*graph.MultiEdge, []*graph.Node, error) {
	return p.real.Build()
}

func (p *Pipeline) String() string {
	return p.real.String()
}
