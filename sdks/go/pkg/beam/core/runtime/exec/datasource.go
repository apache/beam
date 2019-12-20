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
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/ioutilx"
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
	state    StateReader
	index    int64
	splitIdx int64
	start    time.Time

	mu sync.Mutex
}

// ID returns the UnitID for this node.
func (n *DataSource) ID() UnitID {
	return n.UID
}

// Up initializes this datasource.
func (n *DataSource) Up(ctx context.Context) error {
	return nil
}

// StartBundle initializes this datasource for the bundle.
func (n *DataSource) StartBundle(ctx context.Context, id string, data DataContext) error {
	n.mu.Lock()
	n.source = data.Data
	n.state = data.State
	n.start = time.Now()
	n.index = -1
	n.splitIdx = math.MaxInt64
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
		if n.incrementIndexAndCheckSplit() {
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
			case chunk == -1: // State backed iterable!
				chunk, err := coder.DecodeVarInt(r)
				if err != nil {
					return nil, err
				}
				token, err := ioutilx.ReadN(r, (int)(chunk))
				if err != nil {
					return nil, err
				}
				return &concatReStream{
					first: &FixedReStream{Buf: buf},
					next: &proxyReStream{
						open: func() (Stream, error) {
							r, err := n.state.OpenIterable(ctx, n.SID, token)
							if err != nil {
								return nil, err
							}
							return &elementStream{r: r, ec: cv}, nil
						},
					},
				}, nil
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

// FinishBundle resets the source.
func (n *DataSource) FinishBundle(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	log.Infof(ctx, "DataSource: %d elements in %d ns", n.index, time.Now().Sub(n.start))
	n.source = nil
	n.splitIdx = 0 // Ensure errors are returned for split requests if this plan is re-used.
	return n.Out.FinishBundle(ctx)
}

// Down resets the source.
func (n *DataSource) Down(ctx context.Context) error {
	n.source = nil
	return nil
}

func (n *DataSource) String() string {
	return fmt.Sprintf("DataSource[%v, %v] Coder:%v Out:%v", n.SID, n.Name, n.Coder, n.Out.ID())
}

// incrementIndexAndCheckSplit increments DataSource.index by one and checks if
// the caller should abort further element processing, and finish the bundle.
// Returns true if the new value of index is greater than or equal to the split
// index, and false otherwise.
func (n *DataSource) incrementIndexAndCheckSplit() bool {
	b := false
	n.mu.Lock()
	n.index++
	if n.index >= n.splitIdx {
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
	// The count is the number of "completely processed elements"
	// which matches the index of the currently processing element.
	c := n.index
	n.mu.Unlock()
	// Do not sent negative progress reports, index is initialized to 0.
	if c < 0 {
		c = 0
	}
	return ProgressReportSnapshot{ID: n.SID.PtransformID, Name: n.Name, Count: c}
}

// Split takes a sorted set of potential split indices, selects and actuates
// split on an appropriate split index, and returns the selected split index
// if successful. Returns an error when unable to split.
func (n *DataSource) Split(splits []int64, frac float32) (int64, error) {
	if splits == nil {
		return 0, fmt.Errorf("failed to split: requested splits were empty")
	}
	if n == nil {
		return 0, fmt.Errorf("failed to split at requested splits: {%v}, DataSource not initialized", splits)
	}
	n.mu.Lock()
	c := n.index
	// Find the smallest split index that we haven't yet processed, and set
	// the promised split index to this value.
	for _, s := range splits {
		// // Never split on the first element, or the current element.
		if s > 0 && s > c && s <= n.splitIdx {
			n.splitIdx = s
			fs := n.splitIdx
			n.mu.Unlock()
			return fs, nil
		}
	}
	n.mu.Unlock()
	// If we can't find a suitable split index from the requested choices,
	// return an error.
	return 0, fmt.Errorf("failed to split at requested splits: {%v}, DataSource at index: %v", splits, c)
}

type concatReStream struct {
	first, next ReStream
}

func (c *concatReStream) Open() (Stream, error) {
	firstStream, err := c.first.Open()
	if err != nil {
		return nil, err
	}
	return &concatStream{first: firstStream, nextStream: c.next}, nil
}

type concatStream struct {
	first      Stream
	nextStream ReStream
}

// Close nils the stream.
func (s *concatStream) Close() error {
	if s.first == nil {
		return nil
	}
	defer func() {
		s.first = nil
		s.nextStream = nil
	}()
	return s.first.Close()
}

func (s *concatStream) Read() (*FullValue, error) {
	if s.first == nil { // When the stream is closed.
		return nil, io.EOF
	}
	fv, err := s.first.Read()
	if err == nil {
		return fv, nil
	}
	if err == io.EOF {
		if err := s.first.Close(); err != nil {
			s.nextStream = nil
			return nil, err
		}
		if s.nextStream == nil {
			s.first = nil
			return nil, io.EOF
		}
		s.first, err = s.nextStream.Open()
		s.nextStream = nil
		if err != nil {
			return nil, err
		}
		fv, err := s.first.Read()
		return fv, err
	}
	return nil, err
}
