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
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
)

type group struct {
	key    exec.FullValue
	values [][]exec.FullValue
}

// CoGBK buffers all input and continues on FinishBundle. Use with small single-bundle data only.
type CoGBK struct {
	UID  exec.UnitID
	Edge *graph.MultiEdge
	Out  exec.Node

	enc exec.ElementEncoder // key encoder for coder-equality
	m   map[string]*group
}

func (n *CoGBK) ID() exec.UnitID {
	return n.UID
}

func (n *CoGBK) Up(ctx context.Context) error {
	n.enc = exec.MakeElementEncoder(n.Edge.Input[0].From.Coder.Components[0])
	n.m = make(map[string]*group)
	return nil
}

func (n *CoGBK) StartBundle(ctx context.Context, id string, data exec.DataManager) error {
	return n.Out.StartBundle(ctx, id, data)
}

func (n *CoGBK) ProcessElement(ctx context.Context, elm exec.FullValue, _ ...exec.ReStream) error {
	index := elm.Elm.(int)
	value := elm.Elm2.(exec.FullValue)

	var buf bytes.Buffer
	if err := n.enc.Encode(exec.FullValue{Elm: value.Elm}, &buf); err != nil {
		return fmt.Errorf("failed to encode key %v for CoGBK: %v", elm, err)
	}
	key := buf.String()

	g, ok := n.m[key]
	if !ok {
		g = &group{
			key:    exec.FullValue{Elm: value.Elm, Timestamp: value.Timestamp},
			values: make([][]exec.FullValue, len(n.Edge.Input)),
		}
		n.m[key] = g
	}
	g.values[index] = append(g.values[index], exec.FullValue{Elm: value.Elm2, Timestamp: value.Timestamp})
	return nil
}

func (n *CoGBK) FinishBundle(ctx context.Context) error {
	for key, g := range n.m {
		values := make([]exec.ReStream, len(g.values))
		for i, list := range g.values {
			values[i] = &exec.FixedReStream{Buf: list}
		}
		if err := n.Out.ProcessElement(ctx, g.key, values...); err != nil {
			return err
		}
		delete(n.m, key)
	}
	return n.Out.FinishBundle(ctx)
}

func (n *CoGBK) Down(ctx context.Context) error {
	return nil
}

func (n *CoGBK) String() string {
	return fmt.Sprintf("CoGBK. Out:%v", n.Out.ID())
}

// Inject injects the predecessor index into each FullValue, effectively
// converting KV<X,Y> into KV<X,KV<int,Y>>. Used to prime CoGBK.
type Inject struct {
	UID exec.UnitID
	N   int
	Out exec.Node
}

func (n *Inject) ID() exec.UnitID {
	return n.UID
}

func (n *Inject) Up(ctx context.Context) error {
	return nil
}

func (n *Inject) StartBundle(ctx context.Context, id string, data exec.DataManager) error {
	return n.Out.StartBundle(ctx, id, data)
}

func (n *Inject) ProcessElement(ctx context.Context, elm exec.FullValue, values ...exec.ReStream) error {
	return n.Out.ProcessElement(ctx, exec.FullValue{Elm: n.N, Elm2: elm, Timestamp: elm.Timestamp}, values...)
}

func (n *Inject) FinishBundle(ctx context.Context) error {
	return n.Out.FinishBundle(ctx)
}

func (n *Inject) Down(ctx context.Context) error {
	return nil
}

func (n *Inject) String() string {
	return fmt.Sprintf("Inject[%v]. Out:%v", n.N, n.Out.ID())
}
