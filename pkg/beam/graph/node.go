package graph

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
)

// Node is a typed connector, usually corresponding to PCollection<T>. The type
// may however differ, depending on whether it was introduced by GBK output,
// ParDo or "generic" ParDo, for example. The difference matters for the wireup
// at execution time: encoding/decoding must be correctly inserted, if a
// generic ParDo is fused with concretely-typed ones. Also, a generic transform
// may also need re-coding, if the input/output coders are not identical.
type Node struct {
	id int
	// t is the type of underlying data. It must be equal to the coder type.
	// A node type root would always be a WindowedValue.
	t typex.FullType

	Coder *coder.Coder
}

func (n *Node) ID() int {
	return n.id
}

func (n *Node) Type() typex.FullType {
	return n.t
}

func (n *Node) String() string {
	if n.Coder != nil {
		return fmt.Sprintf("{%v: %v/%v}", n.id, n.t, n.Coder)
	} else {
		return fmt.Sprintf("{%v: %v/$}", n.id, n.t)
	}
}

func NodeTypes(list []*Node) []typex.FullType {
	var ret []typex.FullType
	for _, c := range list {
		ret = append(ret, c.Type())
	}
	return ret
}
