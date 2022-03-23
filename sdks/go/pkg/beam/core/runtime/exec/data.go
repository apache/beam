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
	// OpenRead opens a closable byte stream for reading.
	OpenRead(ctx context.Context, id StreamID) (io.ReadCloser, error)
	// OpenWrite opens a closable byte stream for writing.
	OpenWrite(ctx context.Context, id StreamID) (io.WriteCloser, error)
}

// StateReader is the interface for reading side input data.
type StateReader interface {
	// OpenIterableSideInput opens a byte stream for reading iterable side input.
	OpenIterableSideInput(ctx context.Context, id StreamID, sideInputID string, w []byte) (io.ReadCloser, error)
	// OpenMultiMapSideInput opens a byte stream for reading multimap side input.
	OpenMultiMapSideInput(ctx context.Context, id StreamID, sideInputID string, key, w []byte) (io.ReadCloser, error)
	// OpenIterable opens a byte stream for reading unwindowed iterables from the runner.
	OpenIterable(ctx context.Context, id StreamID, key []byte) (io.ReadCloser, error)
	// GetSideInputCache returns the SideInputCache being used at the harness level.
	GetSideInputCache() SideCache
}

// TODO(herohde) 7/20/2018: user state management
