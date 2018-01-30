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
)

// TODO(BEAM-490): This file contains support for the handling of CoGBK
// over the model pipeline. Once it's a primitive, the translation will
// no longer be needed. See graphx/cogbk.go for details.

// Inject injects the predecessor index into each FullValue and encodes
// the value, effectively converting KV<X,Y> into KV<X,KV<int,[]byte>>.
// Used in combination with Expand.
type Inject struct {
	// UID is the unit identifier.
	UID UnitID
	// N is the index (tag) in the union.
	N int
	// ValueCoder is the encoder for the value part of the incoming KV<K,V>.
	ValueEncoder ElementEncoder
	// Out is the successor node.
	Out Node
}

func (n *Inject) ID() UnitID {
	return n.UID
}

func (n *Inject) Up(ctx context.Context) error {
	return nil
}

func (n *Inject) StartBundle(ctx context.Context, id string, data DataManager) error {
	return n.Out.StartBundle(ctx, id, data)
}

func (n *Inject) ProcessElement(ctx context.Context, elm FullValue, values ...ReStream) error {
	// Transform: KV<K,V> to KV<K,KV<int,[]byte>>

	var buf bytes.Buffer
	if err := n.ValueEncoder.Encode(FullValue{Elm: elm.Elm2}, &buf); err != nil {
		return err
	}

	v := FullValue{
		Elm: elm.Elm,
		Elm2: FullValue{
			Elm:  n.N,
			Elm2: buf.Bytes(),
		},
		Timestamp: elm.Timestamp,
	}

	return n.Out.ProcessElement(ctx, v, values...)
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

type Expand struct {
	// UID is the unit identifier.
	UID UnitID

	ValueDecoders []ElementDecoder

	Out Node
}

func (n *Expand) ID() UnitID {
	return n.UID
}

func (n *Expand) Up(ctx context.Context) error {
	return nil
}

func (n *Expand) StartBundle(ctx context.Context, id string, data DataManager) error {
	return n.Out.StartBundle(ctx, id, data)
}

func (n *Expand) ProcessElement(ctx context.Context, elm FullValue, values ...ReStream) error {
	filtered := make([]ReStream, len(n.ValueDecoders))
	for i, dec := range n.ValueDecoders {
		filtered[i] = &filterReStream{n: i, dec: dec, real: values[0]}
	}

	return n.Out.ProcessElement(ctx, elm, filtered...)
}

func (n *Expand) FinishBundle(ctx context.Context) error {
	return n.Out.FinishBundle(ctx)
}

func (n *Expand) Down(ctx context.Context) error {
	return nil
}

func (n *Expand) String() string {
	return fmt.Sprintf("Expand[%v]. Out:%v", len(n.ValueDecoders), n.Out.ID())
}

type filterReStream struct {
	n    int
	dec  ElementDecoder
	real ReStream
}

func (f *filterReStream) Open() Stream {
	return &filterStream{n: f.n, dec: f.dec, real: f.real.Open()}
}

type filterStream struct {
	n    int
	dec  ElementDecoder
	real Stream
}

func (f *filterStream) Close() error {
	return f.real.Close()
}

func (f *filterStream) Read() (FullValue, error) {
	for {
		elm, err := f.real.Read()
		if err != nil {
			return FullValue{}, err
		}

		key := elm.Elm.(int)
		value := elm.Elm2.([]byte)

		// Transform KV<int,[]byte> into V iff key == N

		if key != f.n {
			continue // skip other keys
		}

		v, err := f.dec.Decode(bytes.NewReader(value))
		if err != nil {
			return FullValue{}, fmt.Errorf("failed to decode union value '%v' for key %v: %v", value, key, err)
		}
		v.Timestamp = elm.Timestamp
		return v, nil
	}
}
