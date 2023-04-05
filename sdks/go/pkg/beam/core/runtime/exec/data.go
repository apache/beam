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
)

// Port represents the connection port of external operations.
type Port struct {
	URL string
}

// StreamID represents the static information needed to identify
// a data stream. Dynamic information, notably bundleID, is provided
// implicitly by the managers.
type StreamID struct {
	Port         Port
	PtransformID string
}

func (id StreamID) String() string {
	return fmt.Sprintf("S[%v@%v]", id.PtransformID, id.Port.URL)
}

// DataContext holds connectors to various data connections, incl. state and side input.
type DataContext struct {
	Data  DataManager
	State StateReader
}

// SideCache manages cached ReStream values for side inputs that can be re-used across
// bundles.
type SideCache interface {
	// QueryCache checks the cache for a ReStream corresponding to the transform and
	// side input being used.
	QueryCache(ctx context.Context, transformID, sideInputID string, win, key []byte) ReStream
	// SetCache places a ReStream into the cache for a transform and side input.
	SetCache(ctx context.Context, transformID, sideInputID string, win, key []byte, input ReStream) ReStream
}

// DataManager manages external data byte streams. Each data stream can be
// opened by one consumer only.
type DataManager interface {
	// OpenElementChan opens a channel for data and timers.
	OpenElementChan(ctx context.Context, id StreamID, expectedTimerTransforms []string) (<-chan Elements, error)
	// OpenWrite opens a closable byte stream for data writing.
	OpenWrite(ctx context.Context, id StreamID) (io.WriteCloser, error)
	// OpenTimerWrite opens a byte stream for writing timers
	OpenTimerWrite(ctx context.Context, id StreamID, family string) (io.WriteCloser, error)
}

// StateReader is the interface for reading side input data.
type StateReader interface {
	// OpenIterableSideInput opens a byte stream for reading iterable side input.
	OpenIterableSideInput(ctx context.Context, id StreamID, sideInputID string, w []byte) (io.ReadCloser, error)
	// OpenMultiMapSideInput opens a byte stream for reading multimap side input.
	OpenMultiMapSideInput(ctx context.Context, id StreamID, sideInputID string, key, w []byte) (io.ReadCloser, error)
	// OpenIterable opens a byte stream for reading unwindowed iterables from the runner.
	OpenIterable(ctx context.Context, id StreamID, key []byte) (io.ReadCloser, error)
	// OpenBagUserStateReader opens a byte stream for reading user bag state.
	OpenBagUserStateReader(ctx context.Context, id StreamID, userStateID string, key []byte, w []byte) (io.ReadCloser, error)
	// OpenBagUserStateAppender opens a byte stream for appending user bag state.
	OpenBagUserStateAppender(ctx context.Context, id StreamID, userStateID string, key []byte, w []byte) (io.Writer, error)
	// OpenBagUserStateClearer opens a byte stream for clearing user bag state.
	OpenBagUserStateClearer(ctx context.Context, id StreamID, userStateID string, key []byte, w []byte) (io.Writer, error)
	// OpenMultimapUserStateReader opens a byte stream for reading user multimap state.
	OpenMultimapUserStateReader(ctx context.Context, id StreamID, userStateID string, key []byte, w []byte, mk []byte) (io.ReadCloser, error)
	// OpenMultimapUserStateAppender opens a byte stream for appending user multimap state.
	OpenMultimapUserStateAppender(ctx context.Context, id StreamID, userStateID string, key []byte, w []byte, mk []byte) (io.Writer, error)
	// OpenMultimapUserStateClearer opens a byte stream for clearing user multimap state by key.
	OpenMultimapUserStateClearer(ctx context.Context, id StreamID, userStateID string, key []byte, w []byte, mk []byte) (io.Writer, error)
	// OpenMultimapKeysUserStateReader opens a byte stream for reading the keys of user multimap state.
	OpenMultimapKeysUserStateReader(ctx context.Context, id StreamID, userStateID string, key []byte, w []byte) (io.ReadCloser, error)
	// OpenMultimapKeysUserStateClearer opens a byte stream for clearing all keys of user multimap state.
	OpenMultimapKeysUserStateClearer(ctx context.Context, id StreamID, userStateID string, key []byte, w []byte) (io.Writer, error)
	// GetSideInputCache returns the SideInputCache being used at the harness level.
	GetSideInputCache() SideCache
}

// Elements holds data or timers sent across the data channel.
// If TimerFamilyID is populated, it's a timer, otherwise it's
// data elements.
type Elements struct {
	Data, Timers                []byte
	TimerFamilyID, PtransformID string
}
