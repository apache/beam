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

package direct

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
)

type group struct {
	key    exec.FullValue
	values []exec.FullValue
}

// GBK buffers all input and continues on FinishBundle. Use with small single-bundle data only.
type GBK struct {
	UID  exec.UnitID
	Edge *graph.MultiEdge
	Out  exec.Node

	m map[string]*group
}

func (n *GBK) ID() exec.UnitID {
	return n.UID
}

func (n *GBK) Up(ctx context.Context) error {
	n.m = make(map[string]*group)
	return nil
}

func (n *GBK) StartBundle(ctx context.Context, id string, data exec.DataManager) error {
	return n.Out.StartBundle(ctx, id, data)
}

func (n *GBK) ProcessElement(ctx context.Context, elm exec.FullValue, _ ...exec.ReStream) error {
	var buf bytes.Buffer
	if err := exec.EncodeElement(coder.SkipW(n.Edge.Input[0].From.Coder).Components[0], exec.FullValue{Elm: elm.Elm}, &buf); err != nil {
		return fmt.Errorf("failed to encode key %v for GBK: %v", elm, err)
	}
	key := buf.String()

	g, ok := n.m[key]
	if !ok {
		g = &group{key: exec.FullValue{Elm: elm.Elm, Timestamp: elm.Timestamp}}
		n.m[key] = g
	}
	g.values = append(g.values, exec.FullValue{Elm: elm.Elm2, Timestamp: elm.Timestamp})
	return nil
}

func (n *GBK) FinishBundle(ctx context.Context) error {
	for key, g := range n.m {
		values := &exec.FixedReStream{Buf: g.values}
		if err := n.Out.ProcessElement(ctx, g.key, values); err != nil {
			return err
		}
		delete(n.m, key)
	}
	return n.Out.FinishBundle(ctx)
}

func (n *GBK) Down(ctx context.Context) error {
	return nil
}

func (n *GBK) String() string {
	return fmt.Sprintf("GBK. Out:%v", n.Out.ID())
}
