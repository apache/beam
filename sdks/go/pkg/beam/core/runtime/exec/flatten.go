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

package exec

import (
	"context"
	"fmt"
)

// Flatten is a fan-in node. It ensures that Start/FinishBundle are only
// called once downstream.
type Flatten struct {
	// UID is the unit identifier.
	UID UnitID
	// N is the number of incoming edges.
	N int
	// Out is the output node.
	Out Node

	active bool
	seen   int
}

func (m *Flatten) ID() UnitID {
	return m.UID
}

func (m *Flatten) Up(ctx context.Context) error {
	return nil
}

func (m *Flatten) StartBundle(ctx context.Context, id string, data DataContext) error {
	if m.active {
		return nil // ok: ignore multiple start bundles. We just want the first one.
	}
	m.active = true
	m.seen = 0

	return m.Out.StartBundle(ctx, id, data)
}

func (m *Flatten) ProcessElement(ctx context.Context, elm *FullValue, values ...ReStream) error {
	return m.Out.ProcessElement(ctx, elm, values...)
}

func (m *Flatten) FinishBundle(ctx context.Context) error {
	m.seen++
	if m.seen < m.N {
		return nil // ok: wait for last FinishBundle.
	}
	m.active = false

	return m.Out.FinishBundle(ctx)
}

func (m *Flatten) Down(ctx context.Context) error {
	return nil
}

func (m *Flatten) String() string {
	return fmt.Sprintf("Flatten[%v]. Out:%v", m.N, m.Out)
}
