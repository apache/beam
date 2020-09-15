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
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// ReshuffleInput is a Node.
type ReshuffleInput struct {
	UID   UnitID
	SID   StreamID
	Coder *coder.Coder // Coder for the input PCollection.
	Seed  int64
	Out   Node

	r    *rand.Rand
	enc  ElementEncoder
	wEnc WindowEncoder
	b    bytes.Buffer
	// ret is a cached allocations for passing to the next Unit. Units never modify the passed in FullValue.
	ret FullValue
}

// ID returns the unit debug id.
func (n *ReshuffleInput) ID() UnitID {
	return n.UID
}

// Up initializes the value and window encoders, and the random source.
func (n *ReshuffleInput) Up(ctx context.Context) error {
	n.enc = MakeElementEncoder(coder.SkipW(n.Coder))
	n.wEnc = MakeWindowEncoder(n.Coder.Window)
	n.r = rand.New(rand.NewSource(n.Seed))
	return nil
}

// StartBundle is a no-op.
func (n *ReshuffleInput) StartBundle(ctx context.Context, id string, data DataContext) error {
	return MultiStartBundle(ctx, id, data, n.Out)
}

func (n *ReshuffleInput) ProcessElement(ctx context.Context, value *FullValue, values ...ReStream) error {
	n.b.Reset()
	if err := EncodeWindowedValueHeader(n.wEnc, value.Windows, value.Timestamp, &n.b); err != nil {
		return err
	}
	if err := n.enc.Encode(value, &n.b); err != nil {
		return errors.WithContextf(err, "encoding element %v with coder %v", value, n.Coder)
	}
	n.ret = FullValue{Elm: n.r.Int(), Elm2: n.b.Bytes(), Timestamp: value.Timestamp}
	return n.Out.ProcessElement(ctx, &n.ret)
}

// FinishBundle propagates finish bundle, and clears cached state.
func (n *ReshuffleInput) FinishBundle(ctx context.Context) error {
	n.b = bytes.Buffer{}
	n.ret = FullValue{}
	return MultiFinishBundle(ctx, n.Out)
}

// Down is a no-op.
func (n *ReshuffleInput) Down(ctx context.Context) error {
	return nil
}

func (n *ReshuffleInput) String() string {
	return fmt.Sprintf("ReshuffleInput[%v] Coder:%v", n.SID, n.Coder)
}

// ReshuffleOutput is a Node.
type ReshuffleOutput struct {
	UID   UnitID
	SID   StreamID
	Coder *coder.Coder // Coder for the receiving PCollection.
	Out   Node

	b    bytes.Buffer
	dec  ElementDecoder
	wDec WindowDecoder
	ret  FullValue
}

// ID returns the unit debug id.
func (n *ReshuffleOutput) ID() UnitID {
	return n.UID
}

// Up initializes the value and window encoders, and the random source.
func (n *ReshuffleOutput) Up(ctx context.Context) error {
	n.dec = MakeElementDecoder(coder.SkipW(n.Coder))
	n.wDec = MakeWindowDecoder(n.Coder.Window)
	return nil
}

// StartBundle is a no-op.
func (n *ReshuffleOutput) StartBundle(ctx context.Context, id string, data DataContext) error {
	return MultiStartBundle(ctx, id, data, n.Out)
}

func (n *ReshuffleOutput) ProcessElement(ctx context.Context, value *FullValue, values ...ReStream) error {
	// Marshal the pieces into a temporary buffer since they must be transmitted on FnAPI as a single
	// unit.
	vs, err := values[0].Open()
	if err != nil {
		return errors.WithContextf(err, "decoding values for %v with coder %v", value, n.Coder)
	}
	defer vs.Close()
	for {
		v, err := vs.Read()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return errors.WithContextf(err, "reading values for %v", n)
		}
		n.b = *bytes.NewBuffer(v.Elm.([]byte))
		ws, ts, err := DecodeWindowedValueHeader(n.wDec, &n.b)
		if err != nil {
			return errors.WithContextf(err, "decoding windows for %v", n)
		}
		if err := n.dec.DecodeTo(&n.b, &n.ret); err != nil {
			return errors.WithContextf(err, "decoding element for %v", n)
		}
		n.ret.Windows = ws
		n.ret.Timestamp = ts
		if err := n.Out.ProcessElement(ctx, &n.ret); err != nil {
			return err
		}
	}
}

// FinishBundle propagates finish bundle to downstream nodes.
func (n *ReshuffleOutput) FinishBundle(ctx context.Context) error {
	n.b = bytes.Buffer{}
	n.ret = FullValue{}
	return MultiFinishBundle(ctx, n.Out)
}

// Down is a no-op.
func (n *ReshuffleOutput) Down(ctx context.Context) error {
	return nil
}

func (n *ReshuffleOutput) String() string {
	return fmt.Sprintf("ReshuffleOutput[%v] Coder:%v", n.SID, n.Coder)
}
