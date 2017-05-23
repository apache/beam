package graph

import (
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
)

// Node is a typed connector describing the data type and encoding. A node
// may have multiple inbound and outbound connections. The underlying type
// must be a complete windowed type, i.e., not include any type variables.
type Node struct {
	id int
	// t is the type of underlying data and cannot change. It must be equal to
	// the coder type. A node type root would always be a WindowedValue. The
	// type must be bound, i.e., it cannot contain any type variables.
	t typex.FullType

	// Coder defines the data encoding. It can be changed, but must be of
	// the underlying type, t.
	Coder *coder.Coder
}

// ID returns the graph-local identifier for the node.
func (n *Node) ID() int {
	return n.id
}

// Type returns the underlying full type of the data, such as W<KV<int,string>>.
func (n *Node) Type() typex.FullType {
	return n.t
}

func (n *Node) String() string {
	if n.Coder != nil {
		return fmt.Sprintf("{%v: %v/%v}", n.id, n.t, n.Coder)
	}
	return fmt.Sprintf("{%v: %v/$}", n.id, n.t)
}

// NodeTypes returns the fulltypes of the supplied slice of nodes.
func NodeTypes(list []*Node) []typex.FullType {
	var ret []typex.FullType
	for _, c := range list {
		ret = append(ret, c.Type())
	}
	return ret
}
