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

// CaptureNode is a test Node that captures all elements for verification. It also
// validates that it is invoked correctly.
type CaptureNode struct {
	UID      UnitID
	Elements []FullValue

	status Status
}

func (n *CaptureNode) ID() UnitID {
	return n.UID
}

func (n *CaptureNode) Up(ctx context.Context) error {
	if n.status != Initializing {
		return fmt.Errorf("invalid status for %v: %v, want Initializing", n.UID, n.status)
	}
	n.status = Up
	return nil
}

func (n *CaptureNode) StartBundle(ctx context.Context, id string, data DataManager) error {
	if n.status != Up {
		return fmt.Errorf("invalid status for %v: %v, want Up", n.UID, n.status)
	}
	n.status = Active
	return nil
}

func (n *CaptureNode) ProcessElement(ctx context.Context, elm FullValue, values ...ReStream) error {
	if n.status != Active {
		return fmt.Errorf("invalid status for pardo %v: %v, want Active", n.UID, n.status)
	}

	n.Elements = append(n.Elements, elm)
	return nil
}

func (n *CaptureNode) FinishBundle(ctx context.Context) error {
	if n.status != Active {
		return fmt.Errorf("invalid status for %v: %v, want Active", n.UID, n.status)
	}
	n.status = Up
	return nil
}

func (n *CaptureNode) Down(ctx context.Context) error {
	if n.status != Up {
		return fmt.Errorf("invalid status for %v: %v, want Up", n.UID, n.status)
	}
	n.status = Down
	return nil
}

// FixedRoot is a test Root that emits a fixed number of elements.
type FixedRoot struct {
	UID      UnitID
	Elements []FullValue
	Out      Node
}

func (n *FixedRoot) ID() UnitID {
	return n.UID
}

func (n *FixedRoot) Up(ctx context.Context) error {
	return nil
}

func (n *FixedRoot) StartBundle(ctx context.Context, id string, data DataManager) error {
	return n.Out.StartBundle(ctx, id, data)
}

func (n *FixedRoot) Process(ctx context.Context) error {
	for _, elm := range n.Elements {
		if err := n.Out.ProcessElement(ctx, elm); err != nil {
			return err
		}
	}
	return nil
}

func (n *FixedRoot) FinishBundle(ctx context.Context) error {
	return n.Out.FinishBundle(ctx)
}

func (n *FixedRoot) Down(ctx context.Context) error {
	return nil
}
