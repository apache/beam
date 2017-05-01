package beam

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
)

// PCollection is a value-type, wrapping a graph Node. If there is no node, the
// collection is invalid.
type PCollection struct {
	n *graph.Node
}

func (p PCollection) IsValid() bool {
	return p.n != nil
}

func (p PCollection) Type() typex.FullType {
	if !p.IsValid() {
		panic("Invalid PCollection")
	}
	return p.n.Type()
}

func (p PCollection) Coder() Coder {
	if !p.IsValid() {
		panic("Invalid PCollection")
	}
	return Coder{p.n.Coder}
}

func (p PCollection) SetCoder(c Coder) error {
	if !p.IsValid() {
		panic("Invalid PCollection")
	}

	if !typex.IsEqual(p.n.Type(), c.coder.T) {
		return fmt.Errorf("coder type %v must be identical to node type %v", c.coder.T, p.n)
	}
	p.n.Coder = c.coder
	return nil
}

func (p PCollection) String() string {
	if !p.IsValid() {
		return "(invalid)"
	}
	return p.n.String()
}
