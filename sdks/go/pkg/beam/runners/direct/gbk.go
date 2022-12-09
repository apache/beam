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
	"sort"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
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

	enc  exec.ElementEncoder // key encoder for coder-equality
	wEnc exec.WindowEncoder  // window encoder for windowing
	m    map[string]*group
	wins []typex.Window
}

func (n *CoGBK) ID() exec.UnitID {
	return n.UID
}

func (n *CoGBK) Up(ctx context.Context) error {
	n.enc = exec.MakeElementEncoder(n.Edge.Input[0].From.Coder.Components[0])
	n.wEnc = exec.MakeWindowEncoder(n.Edge.Input[0].From.WindowingStrategy().Fn.Coder())
	n.m = make(map[string]*group)
	return nil
}

func (n *CoGBK) StartBundle(ctx context.Context, id string, data exec.DataContext) error {
	return n.Out.StartBundle(ctx, id, data)
}

func (n *CoGBK) ProcessElement(ctx context.Context, elm *exec.FullValue, _ ...exec.ReStream) error {
	index := elm.Elm2.(*exec.FullValue).Elm.(int)
	value := elm.Elm2.(*exec.FullValue).Elm2

	for _, w := range elm.Windows {
		ws := []typex.Window{w}
		n.wins = append(n.wins, ws...)

		g, err := n.getGroup(n.m, elm, ws)
		if err != nil {
			return errors.Errorf("failed getGroup for %v: %v", elm, err)
		}
		g.values[index] = append(g.values[index], exec.FullValue{Elm: value, Timestamp: elm.Timestamp})
	}
	return nil
}

func (n *CoGBK) getGroup(m map[string]*group, elm *exec.FullValue, ws []typex.Window) (*group, error) {
	var buf bytes.Buffer
	if err := n.enc.Encode(&exec.FullValue{Elm: elm.Elm}, &buf); err != nil {
		return nil, errors.WithContextf(err, "encoding key %v for CoGBK", elm)
	}
	if err := n.wEnc.Encode(ws, &buf); err != nil {
		return nil, errors.WithContextf(err, "encoding window %v for CoGBK", ws)
	}
	key := buf.String()
	g, ok := m[key]
	if !ok {
		g = &group{
			key:    exec.FullValue{Elm: elm.Elm, Timestamp: elm.Timestamp, Windows: ws},
			values: make([][]exec.FullValue, len(n.Edge.Input)),
		}
		m[key] = g
	}
	return g, nil
}

func (n *CoGBK) FinishBundle(ctx context.Context) error {
	winKind := n.Edge.Input[0].From.WindowingStrategy().Fn.Kind
	if winKind == window.Sessions {
		mergeMap, mergeErr := n.mergeWindows()
		if mergeErr != nil {
			return errors.Errorf("failed to merge windows, got: %v", mergeErr)
		}
		if reprocessErr := n.reprocessByWindow(mergeMap); reprocessErr != nil {
			return errors.Errorf("failed to reprocess with merged windows, got :%v", reprocessErr)
		}
	}
	for key, g := range n.m {
		values := make([]exec.ReStream, len(g.values))
		for i, list := range g.values {
			values[i] = &exec.FixedReStream{Buf: list}
		}
		if err := n.Out.ProcessElement(ctx, &g.key, values...); err != nil {
			return err
		}
		delete(n.m, key)
	}
	return n.Out.FinishBundle(ctx)
}

func (n *CoGBK) mergeWindows() (map[typex.Window]int, error) {
	sort.Slice(n.wins, func(i int, j int) bool {
		return n.wins[i].MaxTimestamp() < n.wins[j].MaxTimestamp()
	})
	// mergeMap is a map from the oringal windows to the index of the new window
	// in the mergedWins slice
	mergeMap := make(map[typex.Window]int)
	var mergedWins []typex.Window
	for i := 0; i < len(n.wins); {
		intWin, ok := n.wins[i].(window.IntervalWindow)
		if !ok {
			return nil, errors.Errorf("tried to merge non-interval window type %T", n.wins[i])
		}
		mergeStart := intWin.Start
		mergeEnd := intWin.End
		j := i + 1
		for j < len(n.wins) {
			candidateWin := n.wins[j].(window.IntervalWindow)
			if candidateWin.Start <= mergeEnd {
				mergeEnd = candidateWin.End
				j++
			} else {
				break
			}
		}
		for k := i; k < j; k++ {
			mergeMap[n.wins[k]] = len(mergedWins)
		}
		mergedWins = append(mergedWins, window.IntervalWindow{Start: mergeStart, End: mergeEnd})
		i = j
	}
	n.wins = mergedWins
	return mergeMap, nil
}

func (n *CoGBK) reprocessByWindow(mergeMap map[typex.Window]int) error {
	newGroups := make(map[string]*group)
	for _, g := range n.m {
		ws := []typex.Window{n.wins[mergeMap[g.key.Windows[0]]]}
		gr, err := n.getGroup(newGroups, &g.key, ws)
		if err != nil {
			return errors.Errorf("failed encoding key for %v: %v", g.key.Elm, err)
		}
		for i, list := range g.values {
			gr.values[i] = append(gr.values[i], list...)
		}
	}
	n.m = newGroups
	return nil
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

func (n *Inject) StartBundle(ctx context.Context, id string, data exec.DataContext) error {
	return n.Out.StartBundle(ctx, id, data)
}

func (n *Inject) ProcessElement(ctx context.Context, elm *exec.FullValue, values ...exec.ReStream) error {
	v := *elm
	v.Elm = n.N
	return n.Out.ProcessElement(ctx, &exec.FullValue{
		Elm:       elm.Elm,
		Elm2:      &v,
		Timestamp: elm.Timestamp,
		Windows:   elm.Windows,
		Pane:      elm.Pane,
	}, values...)
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
