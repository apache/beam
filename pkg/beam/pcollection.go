package beam

import (
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"reflect"
)

// PCollection is a value-type, wrapping a Graph Node.
type PCollection struct {
	n *graph.Node
}

func (p PCollection) Type() reflect.Type {
	return p.n.T
}

func (p PCollection) Coder() Coder {
	return Coder{p.n.Coder}
}

func (p PCollection) SetCoder(coder Coder) error {
	// TODO(herohde): validate that the coder is compatible with the underlying type.
	// Perhaps we need to do that when everything is stabilitized.

	p.n.Coder = coder.coder
	return nil
}

// func (p PCollection) KV() (reflect.Type, reflect.Type, bool) {
//	return nil, nil, false
// }

func (p PCollection) String() string {
	return p.n.String()
}
