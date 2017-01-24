package beam

import (
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/model"
)

// TODO(herohde): we could also just leave Pipeline a value-type here.

// Pipeline represents the Beam deferred execution Graph as it is being
// constructed. It is thus essentially an encapsulated low-level Graph Graph
// that maintains a scoped insertion point for composite transforms.
type Pipeline struct {
	parent *graph.Scope
	real   *graph.Graph
}

// NewPipeline creates a new empty beam pipeline.
func NewPipeline() *Pipeline {
	real := graph.New()
	return &Pipeline{real.Root(), real}
}

// TODO(herohde): add composite helper that picks up the enclosing function name.

// Composite returns a Pipeline scoped as a composite transform. The underlying
// deferred execution Graph is the same.
func (p *Pipeline) Composite(name string) *Pipeline {
	scope := p.real.NewScope(p.parent, name)
	return &Pipeline{scope, p.real}
}

func (p *Pipeline) Build() (*model.Pipeline, error) {
	return p.real.Build()
}

// TODO(herohde): remove FakeBuild

func (p *Pipeline) FakeBuild() map[int]*graph.MultiEdge {
	return p.real.FakeBuild()
}
