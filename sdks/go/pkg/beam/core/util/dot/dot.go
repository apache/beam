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

// Package dot produces DOT graphs from Beam graph representations.
package dot

import (
	"fmt"
	"io"
	"path"
	"text/template"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

var (
	header = `
digraph execution_plan {
  label="execution_plan"
  labeljust="l";
  fontname="Ubuntu";
  fontsize="13";
  bgcolor="lightgray";
  style="solid";
  penwidth="0.5";
  concentrate="true";

  // Node definition used for multiedge
  node [shape="rectangle" style="filled" fillcolor="honeydew" fontname="Ubuntu" penwidth="1.0" margin="0.05,0.0.05"];

  bgcolor="#e6ecfa";
`

	nodeText = `  "{{.Name}}" [ shape="ellipse" fillcolor = "lightblue" label="{{.Label}}"]
`
	edgeDefnText = `  "{{.Name}}" [ label="{{.Label}}" ]
`
	edgeText = `  "{{.From}}" -> "{{.To}}"
`
	footer = `
}
`
	nodeTmpl     = template.Must(template.New("node").Parse(nodeText))
	edgeDefnTmpl = template.Must(template.New("edge_defn").Parse(edgeDefnText))
	edgeTmpl     = template.Must(template.New("edge").Parse(edgeText))
)

type nodeLinks struct {
	From *graph.Outbound
	To   *graph.Inbound
	Node *graph.Node
}

func (n *nodeLinks) String() string {
	status := ""
	if n.Node == nil {
		status = "UNKNOWN NODE\n"
	}

	if n.From != nil && n.To != nil {
		return fmt.Sprintf("%sModel: %d: %v\nAs: %d: %v", status, n.To.From.ID(), n.To.Type, n.From.To.ID(), n.From.Type)
	}

	if n.To != nil {
		return fmt.Sprintf("%sModel: %d: %v", status, n.To.From.ID(), n.To.Type)
	}

	if n.From != nil {
		return fmt.Sprintf("%sAs: %d %v", status, n.From.To.ID(), n.From.Type)
	}

	return fmt.Sprintf("%sUNCONNECTED NODE", status)
}

// Render produces a DOT-compatible representation of the graph into the supplied io.Writer
func Render(edges []*graph.MultiEdge, nodes []*graph.Node, w io.Writer) error {
	uniqNodes := make(map[*graph.Node]*nodeLinks)

	// Create a lookup structure for all the created nodes. This helps identify
	// unconnected nodes (which shouldn't happen in practice).
	for _, node := range nodes {
		uniqNodes[node] = &nodeLinks{Node: node, From: nil, To: nil}
	}

	// Extract the node connectivity from the graph
	for _, edge := range edges {
		for _, ib := range edge.Input {
			uniqNodes[ib.From].To = ib
		}
		for _, ob := range edge.Output {
			uniqNodes[ob.To].From = ob
		}
	}

	// Render the graph elements: nodes and the edges
	w.Write([]byte(header))
	seen := make(map[*graph.Node]bool)
	for _, node := range nodes {
		if seen[node] {
			continue
		}
		seen[node] = true
		err := nodeTmpl.Execute(w, struct{ Name, Label string }{node.String(), uniqNodes[node].String()})
		if err != nil {
			return err
		}
	}

	for _, edge := range edges {
		e := fmt.Sprintf("%d: %s", edge.ID(), edge.Op)
		label := fmt.Sprint(edge.Op)
		if name := path.Base(edge.Name()); name != label {
			label = fmt.Sprintf("%s\n%s", edge.Op, name)
		}
		if err := edgeDefnTmpl.Execute(w, struct{ Name, Label string }{e, label}); err != nil {
			return errors.Wrap(err, "render DOT failed")
		}
		for _, ib := range edge.Input {
			err := edgeTmpl.Execute(w, struct{ From, To string }{ib.From.String(), e})
			if err != nil {
				return errors.Wrap(err, "render DOT failed")
			}
		}
		for _, ob := range edge.Output {
			uniqNodes[ob.To].From = ob
			err := edgeTmpl.Execute(w, struct{ From, To string }{e, ob.To.String()})
			if err != nil {
				return errors.Wrap(err, "render DOT failed")
			}
		}
	}
	w.Write([]byte(footer))
	return nil
}
