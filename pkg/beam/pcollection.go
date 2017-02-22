package beam

import (
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"reflect"
)

// PCollection is a value-type, wrapping a Graph Node. If there is no node, the
// collection is invalid.
type PCollection struct {
	n *graph.Node
}

func (p PCollection) IsValid() bool {
	return p.n != nil
}

func (p PCollection) Type() reflect.Type {
	if !p.IsValid() {
		panic("Invalid PCollection")
	}
	return p.n.T
}

func (p PCollection) Coder() Coder {
	if !p.IsValid() {
		panic("Invalid PCollection")
	}
	return Coder{p.n.Coder}
}

func (p PCollection) SetCoder(coder Coder) error {
	if !p.IsValid() {
		panic("Invalid PCollection")
	}

	// TODO(herohde): validate that the coder is compatible with the underlying type.
	// Perhaps we need to do that when everything is stabilized.

	p.n.Coder = coder.coder
	return nil
}

// func (p PCollection) KV() (reflect.Type, reflect.Type, bool) {
//	return nil, nil, false
// }

func (p PCollection) String() string {
	if !p.IsValid() {
		return "(invalid)"
	}
	return p.n.String()
}
