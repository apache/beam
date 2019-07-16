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
	"math"
	"sync"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

// DataSource is a Root execution unit.
type DataSource struct {
	UID   UnitID
	SID   StreamID
	Name  string
	Coder *coder.Coder
	Out   Node

	source   DataManager
	count    int64
	splitPos int64
	start    time.Time

	mu sync.Mutex
}

func (n *DataSource) ID() UnitID {
	return n.UID
}

func (n *DataSource) Up(ctx context.Context) error {
	return nil
}

func (n *DataSource) StartBundle(ctx context.Context, id string, data DataContext) error {
	n.mu.Lock()
	n.source = data.Data
	n.start = time.Now()
	n.count = 0
	n.splitPos = math.MaxInt64
	n.mu.Unlock()
	return n.Out.StartBundle(ctx, id, data)
}

// Process opens the data source, reads and decodes data, kicking off element processing.
func (n *DataSource) Process(ctx context.Context) error {
	r, err := n.source.OpenRead(ctx, n.SID)
	if err != nil {
		return err
	}
	defer r.Close()

	c := coder.SkipW(n.Coder)
	wc := MakeWindowDecoder(n.Coder.Window)

	var cp ElementDecoder    // Decoder for the primary element or the key in CoGBKs.
	var cvs []ElementDecoder // Decoders for each value stream in CoGBKs.

	switch {
	case coder.IsCoGBK(c):
		cp = MakeElementDecoder(c.Components[0])

		// TODO(BEAM-490): Support multiple value streams (coder components) with
		// with CoGBK.
		cvs = []ElementDecoder{MakeElementDecoder(c.Components[1])}
	default:
		cp = MakeElementDecoder(c)
	}

	for {
		if n.IncrementCountAndCheckSplit(ctx) {
			return nil
		}
		ws, t, err := DecodeWindowedValueHeader(wc, r)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return errors.Wrap(err, "source failed")
		}

		// Decode key or parallel element.
		pe, err := cp.Decode(r)
		if err != nil {
			return errors.Wrap(err, "source decode failed")
		}
		pe.Timestamp = t
		pe.Windows = ws

		var valReStreams []ReStream
		for _, cv := range cvs {
			values, err := n.makeReStream(ctx, pe, cv, r)
			if err != nil {
				return err
			}
			valReStreams = append(valReStreams, values)
		}

		if err := n.Out.ProcessElement(ctx, pe, valReStreams...); err != nil {
			return err
		}
	}
}

func (n *DataSource) makeReStream(ctx context.Context, key *FullValue, cv ElementDecoder, r io.ReadCloser) (ReStream, error) {
	size, err := coder.DecodeInt32(r)
	if err != nil {
		return nil, errors.Wrap(err, "stream size decoding failed")
	}

	switch {
	case size >= 0:
		// Single chunk streams are fully read in and buffered in memory.
		var buf []FullValue
		buf, err = readStreamToBuffer(cv, r, int64(size), buf)
		if err != nil {
			return nil, err
		}
		return &FixedReStream{Buf: buf}, nil
	case size == -1: // Shouldn't this be 0?
		// Multi-chunked stream.
		var buf []FullValue
		for {
			chunk, err := coder.DecodeVarInt(r)
			if err != nil {
				return nil, errors.Wrap(err, "stream chunk size decoding failed")
			}
			// All done, escape out.
			switch {
			case chunk == 0: // End of stream, return buffer.
				return &FixedReStream{Buf: buf}, nil
			case chunk > 0: // Non-zero chunk, read that many elements from the stream, and buffer them.
				buf, err = readStreamToBuffer(cv, r, chunk, buf)
				if err != nil {
					return nil, err
				}
			default:
				return nil, errors.Errorf("multi-chunk stream with invalid chunk size of %d", chunk)
			}
		}
	default:
		return nil, errors.Errorf("received stream with marker size of %d", size)
	}
}

func readStreamToBuffer(cv ElementDecoder, r io.ReadCloser, size int64, buf []FullValue) ([]FullValue, error) {
	for i := int64(0); i < size; i++ {
		value, err := cv.Decode(r)
		if err != nil {
			return nil, errors.Wrap(err, "stream value decode failed")
		}
		buf = append(buf, *value)
	}
	return buf, nil
}

func (n *DataSource) FinishBundle(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	log.Infof(ctx, "DataSource: %d elements in %d ns", n.count, time.Now().Sub(n.start))
	n.source = nil
	err := n.Out.FinishBundle(ctx)
	n.count = 0
	n.splitPos = math.MaxInt64
	return err
}

func (n *DataSource) Down(ctx context.Context) error {
	n.source = nil
	return nil
}

func (n *DataSource) String() string {
	return fmt.Sprintf("DataSource[%v, %v] Coder:%v Out:%v", n.SID, n.Name, n.Coder, n.Out.ID())
}

// IncrementCountAndCheckSplit increments DataSource.count by one and checks if
// the caller should abort further element processing, and finish the bundle.
// Returns true if the new value of count is greater than or equal to the split
// point, and false otherwise.
func (n *DataSource) IncrementCountAndCheckSplit(ctx context.Context) bool {
	b := false
	n.mu.Lock()
	n.count++
	if n.count >= n.splitPos {
		b = true
	}
	n.mu.Unlock()
	return b
}

// ProgressReportSnapshot captures the progress reading an input source.
type ProgressReportSnapshot struct {
	ID, Name string
	Count    int64
}

// Progress returns a snapshot of the source's progress.
func (n *DataSource) Progress() ProgressReportSnapshot {
	if n == nil {
		return ProgressReportSnapshot{}
	}
	n.mu.Lock()
	c := n.count
	n.mu.Unlock()
	return ProgressReportSnapshot{n.SID.PtransformID, n.Name, c}
}

// Split takes a sorted set of potential split points, selects and actuates
// split on an appropriate split point, and returns the selected split point
// if successful. Returns an error when unable to split.
func (n *DataSource) Split(splits []int64, frac float32) (int64, error) {
	if splits == nil {
		return 0, fmt.Errorf("failed to split: requested splits were empty")
	}
	if n == nil {
		return 0, fmt.Errorf("failed to split at requested splits: {%v}, DataSource not initialized", splits)
	}
	n.mu.Lock()
	c := n.count
	// Find the smallest split index that we haven't yet processed, and set
	// the promised split position to this value.
	for _, s := range splits {
		if s >= c && s < n.splitPos {
			n.splitPos = s
			fs := n.splitPos
			n.mu.Unlock()
			return fs, nil
		}
	}
	n.mu.Unlock()
	// If we can't find a suitable split point from the requested choices,
	// return an error.
	return 0, fmt.Errorf("failed to split at requested splits: {%v}, DataSource at index: %v", splits, c)
}
