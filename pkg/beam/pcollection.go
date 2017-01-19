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

func (p PCollection) Coder() string {
	return p.n.Coder
}

func (p PCollection) SetCoder(id string) {
	p.n.Coder = id
}

// func (p PCollection) KV() (reflect.Type, reflect.Type, bool) {
//	return nil, nil, false
// }

func (p PCollection) String() string {
	return p.n.String()
}
