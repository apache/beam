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
	"context"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
)

const fixedKey int64 = 0

// AddFixedKey attaches a fixed key of 0 to all inbound elements.
// Used to enable global combine behavior through a per-key process
// for the Go Direct runner.
type AddFixedKey struct {
	UID exec.UnitID
	Out exec.Node
}

// ID returns the UnitID for the AddFixedKey node.
func (n *AddFixedKey) ID() exec.UnitID {
	return n.UID
}

// Up is a no-op for AddFixedKey.
func (n *AddFixedKey) Up(ctx context.Context) error {
	return nil
}

// StartBundle calls the output node's StartBundle method.
func (n *AddFixedKey) StartBundle(ctx context.Context, id string, data exec.DataContext) error {
	return n.Out.StartBundle(ctx, id, data)
}

// ProcessElement adds a fixed key of 0 and moves the contents of the given FullValue into the Value portion
// of a new FullValue struct.
func (n *AddFixedKey) ProcessElement(ctx context.Context, elm *exec.FullValue, _ ...exec.ReStream) error {
	var keyedVal exec.FullValue
	if elm.Elm2 == nil {
		keyedVal = exec.FullValue{Elm: fixedKey, Elm2: elm.Elm, Timestamp: elm.Timestamp, Windows: elm.Windows, Pane: elm.Pane}
	} else {
		keyedVal = exec.FullValue{Elm: fixedKey, Elm2: elm, Timestamp: elm.Timestamp, Windows: elm.Windows, Pane: elm.Pane}
	}
	return n.Out.ProcessElement(ctx, &keyedVal)
}

// FinishBundle calls the output node's FinishBundle method.
func (n *AddFixedKey) FinishBundle(ctx context.Context) error {
	return n.Out.FinishBundle(ctx)
}

// Down is a no-op for AddFixedKey.
func (n *AddFixedKey) Down(ctx context.Context) error {
	return nil
}

func (n *AddFixedKey) String() string {
	return fmt.Sprintf("AddFixedKey Out:%v", n.Out.ID())
}

// DropKey removes the attached key from inbound elements.
// Used to enable global combine behavior through a per-key process
// for the Go Direct runner.
type DropKey struct {
	UID exec.UnitID
	Out exec.Node
}

// ID returns the UnitID for the DropKey exec node.
func (n *DropKey) ID() exec.UnitID {
	return n.UID
}

// Up is a no-op for DropKey.
func (n *DropKey) Up(ctx context.Context) error {
	return nil
}

// StartBundle calls the output node's StartBundle method.
func (n *DropKey) StartBundle(ctx context.Context, id string, data exec.DataContext) error {
	return n.Out.StartBundle(ctx, id, data)
}

// ProcessElement strips the key off of the given FullValue and moves Value element into the main element position.
func (n *DropKey) ProcessElement(ctx context.Context, elm *exec.FullValue, _ ...exec.ReStream) error {
	droppedKey, ok := elm.Elm2.(exec.FullValue)
	// If not dealing with a nested FullValue, pull the value out and make a new FullValue.
	if !ok {
		droppedKey = exec.FullValue{Elm: elm.Elm2, Elm2: nil, Timestamp: elm.Timestamp, Windows: elm.Windows, Pane: elm.Pane}
	}
	return n.Out.ProcessElement(ctx, &droppedKey)
}

// FinishBundle calls the output node's FinishBundle method.
func (n *DropKey) FinishBundle(ctx context.Context) error {
	return n.Out.FinishBundle(ctx)
}

// Down is a no-op for DropKey.
func (n *DropKey) Down(ctx context.Context) error {
	return nil
}

func (n *DropKey) String() string {
	return fmt.Sprintf("DropKey Out:%v", n.Out.ID())
}
