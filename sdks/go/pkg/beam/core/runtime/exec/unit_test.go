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
	"io"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
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
		return errors.Errorf("invalid status for %v: %v, want Initializing", n.UID, n.status)
	}
	n.status = Up
	return nil
}

func (n *CaptureNode) StartBundle(ctx context.Context, id string, data DataContext) error {
	if n.status != Up {
		return errors.Errorf("invalid status for %v: %v, want Up", n.UID, n.status)
	}
	n.status = Active
	return nil
}

func (n *CaptureNode) ProcessElement(ctx context.Context, elm *FullValue, values ...ReStream) error {
	if n.status != Active {
		return errors.Errorf("invalid status for pardo %v: %v, want Active", n.UID, n.status)
	}

	n.Elements = append(n.Elements, *elm)
	return nil
}

func (n *CaptureNode) FinishBundle(ctx context.Context) error {
	if n.status != Active {
		return errors.Errorf("invalid status for %v: %v, want Active", n.UID, n.status)
	}
	n.status = Up
	return nil
}

func (n *CaptureNode) Down(ctx context.Context) error {
	if n.status != Up {
		return errors.Errorf("invalid status for %v: %v, want Up", n.UID, n.status)
	}
	n.status = Down
	return nil
}

// iterInput keeps a key along with the list of associated values.
type iterInput struct {
	Key    FullValue
	Values []FullValue
}

// IteratorCaptureNode is a test Node that captures all KV pairs elements for
// verification, including all streamed values. It also validates that it is
// invoked correctly.
type IteratorCaptureNode struct {
	CaptureNode    // embedded for the default unit methods
	CapturedInputs []iterInput
}

func (n *IteratorCaptureNode) ProcessElement(ctx context.Context, elm *FullValue, values ...ReStream) error {
	if n.CaptureNode.status != Active {
		return errors.Errorf("invalid status for pardo %v: %v, want Active", n.CaptureNode.UID, n.CaptureNode.status)
	}
	var vs []FullValue
	for _, iterV := range values {
		s, err := iterV.Open()
		if err != nil {
			return err
		}
		defer s.Close()
		v, err := s.Read()
		for err == nil {
			vs = append(vs, *v)
			v, err = s.Read()
		}
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}

	n.CapturedInputs = append(n.CapturedInputs, iterInput{Key: *elm, Values: vs})
	return nil
}

// FixedRoot is a test Root that emits a fixed number of elements.
type FixedRoot struct {
	UID      UnitID
	Elements []MainInput
	Out      Node
}

func (n *FixedRoot) ID() UnitID {
	return n.UID
}

func (n *FixedRoot) Up(ctx context.Context) error {
	return nil
}

func (n *FixedRoot) StartBundle(ctx context.Context, id string, data DataContext) error {
	return n.Out.StartBundle(ctx, id, data)
}

func (n *FixedRoot) Process(ctx context.Context) error {
	for _, elm := range n.Elements {
		if err := n.Out.ProcessElement(ctx, &elm.Key, elm.Values...); err != nil {
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

// FixedSideInputAdapter is an adapter for a fixed ReStream.
type FixedSideInputAdapter struct {
	Val ReStream
}

func (a *FixedSideInputAdapter) NewIterable(ctx context.Context, reader StateReader, w typex.Window) (ReStream, error) {
	return a.Val, nil
}

// BenchRoot is a test Root that emits elements through a channel for benchmarking purposes.
type BenchRoot struct {
	UID      UnitID
	Elements <-chan MainInput
	Out      Node
}

func (n *BenchRoot) ID() UnitID {
	return n.UID
}

func (n *BenchRoot) Up(ctx context.Context) error {
	return nil
}

func (n *BenchRoot) StartBundle(ctx context.Context, id string, data DataContext) error {
	return n.Out.StartBundle(ctx, id, data)
}

func (n *BenchRoot) Process(ctx context.Context) error {
	for elm := range n.Elements {
		if err := n.Out.ProcessElement(ctx, &elm.Key, elm.Values...); err != nil {
			return err
		}
	}
	return nil
}

func (n *BenchRoot) FinishBundle(ctx context.Context) error {
	return n.Out.FinishBundle(ctx)
}

func (n *BenchRoot) Down(ctx context.Context) error {
	return nil
}

// BlockingNode is a test node that blocks execution based on a predicate.
type BlockingNode struct {
	UID     UnitID
	Out     Node
	Block   func(*FullValue) bool
	Unblock <-chan struct{}

	status Status
}

func (n *BlockingNode) ID() UnitID {
	return n.UID
}

func (n *BlockingNode) Up(ctx context.Context) error {
	if n.status != Initializing {
		return errors.Errorf("invalid status for %v: %v, want Initializing", n.UID, n.status)
	}
	n.status = Up
	return nil
}

func (n *BlockingNode) StartBundle(ctx context.Context, id string, data DataContext) error {
	if n.status != Up {
		return errors.Errorf("invalid status for %v: %v, want Up", n.UID, n.status)
	}
	err := n.Out.StartBundle(ctx, id, data)
	n.status = Active
	return err
}

func (n *BlockingNode) ProcessElement(ctx context.Context, elm *FullValue, values ...ReStream) error {
	if n.status != Active {
		return errors.Errorf("invalid status for pardo %v: %v, want Active", n.UID, n.status)
	}
	if n.Block(elm) {
		<-n.Unblock // Block until we get the signal to continue.
	}
	return n.Out.ProcessElement(ctx, elm, values...)
}

func (n *BlockingNode) FinishBundle(ctx context.Context) error {
	if n.status != Active {
		return errors.Errorf("invalid status for %v: %v, want Active", n.UID, n.status)
	}
	err := n.Out.FinishBundle(ctx)
	n.status = Up
	return err
}

func (n *BlockingNode) Down(ctx context.Context) error {
	if n.status != Up {
		return errors.Errorf("invalid status for %v: %v, want Up", n.UID, n.status)
	}
	n.status = Down
	return nil
}
