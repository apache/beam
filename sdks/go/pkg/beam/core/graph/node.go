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

package graph

import (
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

// Node is a typed connector describing the data type and encoding. A node
// may have multiple inbound and outbound connections. The underlying type
// must be a complete type, i.e., not include any type variables.
type Node struct {
	id int
	// t is the type of underlying data and cannot change. It must be equal to
	// the coder type. The type must be bound, i.e., it cannot contain any
	// type variables.
	t typex.FullType

	// Coder defines the data encoding. It can be changed, but must be of
	// the underlying type, t.
	Coder *coder.Coder

	// w defines the kind of windowing used.
	w *window.Window
}

// ID returns the graph-local identifier for the node.
func (n *Node) ID() int {
	return n.id
}

// Type returns the underlying full type of the data, such as KV<int,string>.
func (n *Node) Type() typex.FullType {
	return n.t
}

// Window returns the window applied to the data.
func (n *Node) Window() *window.Window {
	return n.w
}

func (n *Node) String() string {
	return fmt.Sprintf("{%v: %v/%v/%v}", n.id, n.t, n.w, n.Coder)
}

// NodeTypes returns the fulltypes of the supplied slice of nodes.
func NodeTypes(list []*Node) []typex.FullType {
	var ret []typex.FullType
	for _, c := range list {
		ret = append(ret, c.Type())
	}
	return ret
}
