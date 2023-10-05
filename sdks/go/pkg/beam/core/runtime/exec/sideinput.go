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
	"io"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

// This file contains support for side input.

// iterableSideInputKey is the fixed runtime key value for iterable side input.
const iterableSideInputKey = ""

// SideInputAdapter provides a concrete ReStream from a low-level side input reader. It
// encapsulates StreamID and coding as needed.
type SideInputAdapter interface {
	NewIterable(ctx context.Context, reader StateReader, w typex.Window) (ReStream, error)
	NewKeyedIterable(ctx context.Context, reader StateReader, w typex.Window, iterKey any) (ReStream, error)
}

type sideInputAdapter struct {
	sid         StreamID
	sideInputID string
	wc          WindowEncoder
	kc          ElementEncoder
	ec          ElementDecoder
	wm          WindowMapper
	c           *coder.Coder
}

// NewSideInputAdapter returns a side input adapter for the given StreamID and coder.
// It expects a W<V> or W<KV<K,V>> coder, because the protocol requires windowing information.
func NewSideInputAdapter(sid StreamID, sideInputID string, c *coder.Coder, wm WindowMapper) SideInputAdapter {
	if !coder.IsW(c) {
		panic(fmt.Sprintf("expected WV coder for side input %v: %v", sid, c))
	}

	wc := MakeWindowEncoder(c.Window)
	var kc ElementEncoder
	var ec ElementDecoder
	if coder.IsKV(coder.SkipW(c)) {
		kc = MakeElementEncoder(coder.SkipW(c).Components[0])
		ec = MakeElementDecoder(coder.SkipW(c).Components[1])
	} else {
		ec = MakeElementDecoder(coder.SkipW(c))
	}
	return &sideInputAdapter{sid: sid, sideInputID: sideInputID, wc: wc, kc: kc, ec: ec, wm: wm, c: c}
}

// NewIterable returns a ReStream of an iterable side input from the runner, either by getting the ReStream from
// the side input cache or by opening a new stream and reading it in.
func (s *sideInputAdapter) NewIterable(ctx context.Context, reader StateReader, w typex.Window) (ReStream, error) {
	key := []byte(iterableSideInputKey)

	// Catch if iterable values themselves are KV encoded, update element decoder.
	if s.kc != nil {
		s.ec = MakeElementDecoder(coder.SkipW(s.c))
	}

	mw, err := s.wm.MapWindow(w)
	if err != nil {
		return nil, err
	}
	win, err := EncodeWindow(s.wc, mw)
	if err != nil {
		return nil, err
	}
	cache := reader.GetSideInputCache()
	// Cache hit
	if r := cache.QueryCache(ctx, s.sid.PtransformID, s.sideInputID, win, key); r != nil {
		return r, nil
	}

	// Cache miss, build new ReStream
	r := &proxyReStream{
		open: func() (Stream, error) {
			r, err := reader.OpenIterableSideInput(ctx, s.sid, s.sideInputID, win)
			if err != nil {
				return nil, err
			}
			return &elementStream{r: r, ec: s.ec}, nil
		},
	}
	return cache.SetCache(ctx, s.sid.PtransformID, s.sideInputID, win, key, r), nil
}

// NewKeyedIterable returns a ReStream of a multimap side input from the runner, either by getting the ReStream from
// the side input cache or by opening a new stream and reading it in.
func (s *sideInputAdapter) NewKeyedIterable(ctx context.Context, reader StateReader, w typex.Window, iterKey any) (ReStream, error) {
	if s.kc == nil {
		return nil, fmt.Errorf("cannot make a keyed iterable for an unkeyed side input %v", s.sideInputID)
	}
	key, err := EncodeElement(s.kc, iterKey)
	if err != nil {
		return nil, err
	}
	mw, err := s.wm.MapWindow(w)
	if err != nil {
		return nil, err
	}
	win, err := EncodeWindow(s.wc, mw)
	if err != nil {
		return nil, err
	}
	cache := reader.GetSideInputCache()
	// Cache hit
	if r := cache.QueryCache(ctx, s.sid.PtransformID, s.sideInputID, win, key); r != nil {
		return r, nil
	}

	// Cache miss, build new ReStream
	r := &proxyReStream{
		open: func() (Stream, error) {
			r, err := reader.OpenMultiMapSideInput(ctx, s.sid, s.sideInputID, key, win)
			if err != nil {
				return nil, err
			}
			return &elementStream{r: r, ec: s.ec}, nil
		},
	}
	return cache.SetCache(ctx, s.sid.PtransformID, s.sideInputID, win, key, r), nil
}

func (s *sideInputAdapter) String() string {
	return fmt.Sprintf("SideInputAdapter[%v, %v] - Coder %v", s.sid, s.sideInputID, s.c)
}

// proxyReStream is a simple wrapper of an open function.
type proxyReStream struct {
	open func() (Stream, error)
}

func (p *proxyReStream) Open() (Stream, error) {
	return p.open()
}

// elementStream exposes a Stream from decoding elements.
type elementStream struct {
	r  io.ReadCloser
	ec ElementDecoder
}

func (s *elementStream) Close() error {
	return s.r.Close()
}

func (s *elementStream) Read() (*FullValue, error) {
	// We should see a stream of unwindowed values -- no sizes, no key.
	return s.ec.Decode(s.r)
}

// FixedKey transform any value into KV<K, V> for a fixed K.
type FixedKey struct {
	// UID is the unit identifier.
	UID UnitID
	// Key is the given key
	Key any
	// Out is the successor node.
	Out Node
}

func (n *FixedKey) ID() UnitID {
	return n.UID
}

func (n *FixedKey) Up(ctx context.Context) error {
	return nil
}

func (n *FixedKey) StartBundle(ctx context.Context, id string, data DataContext) error {
	return n.Out.StartBundle(ctx, id, data)
}

func (n *FixedKey) ProcessElement(ctx context.Context, elm *FullValue, values ...ReStream) error {
	// Transform: V to KV<K,V>

	v := &FullValue{
		Elm:       n.Key,
		Elm2:      elm,
		Timestamp: elm.Timestamp,
		Windows:   elm.Windows,
	}
	return n.Out.ProcessElement(ctx, v, values...)
}

func (n *FixedKey) FinishBundle(ctx context.Context) error {
	return n.Out.FinishBundle(ctx)
}

func (n *FixedKey) Down(ctx context.Context) error {
	return nil
}

func (n *FixedKey) String() string {
	return fmt.Sprintf("FixedKey[%v]. Out:%v", n.Key, n.Out.ID())
}
