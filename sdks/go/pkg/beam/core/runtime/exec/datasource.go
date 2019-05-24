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

func (n *DataSource) Process(ctx context.Context) error {
	r, err := n.source.OpenRead(ctx, n.SID)
	if err != nil {
		return err
	}
	defer r.Close()

	c := coder.SkipW(n.Coder)
	wc := MakeWindowDecoder(n.Coder.Window)

	switch {
	case coder.IsCoGBK(c):
		ck := MakeElementDecoder(c.Components[0])
		cv := MakeElementDecoder(c.Components[1])

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

			// Decode key

			key, err := ck.Decode(r)
			if err != nil {
				return errors.Wrap(err, "source decode failed")
			}
			key.Timestamp = t
			key.Windows = ws

			// TODO(herohde) 4/30/2017: the State API will be handle re-iterations
			// and only "small" value streams would be inline. Presumably, that
			// would entail buffering the whole stream. We do that for now.

			var buf []FullValue

			size, err := coder.DecodeInt32(r)
			if err != nil {
				return errors.Wrap(err, "stream size decoding failed")
			}

			if size > -1 {
				// Single chunk stream.

				// log.Printf("Fixed size=%v", size)
				for i := int32(0); i < size; i++ {
					value, err := cv.Decode(r)
					if err != nil {
						return errors.Wrap(err, "stream value decode failed")
					}
					buf = append(buf, *value)
				}
			} else {
				// Multi-chunked stream.

				for {
					chunk, err := coder.DecodeVarUint64(r)
					if err != nil {
						return errors.Wrap(err, "stream chunk size decoding failed")
					}

					// log.Printf("Chunk size=%v", chunk)

					if chunk == 0 {
						break
					}

					for i := uint64(0); i < chunk; i++ {
						value, err := cv.Decode(r)
						if err != nil {
							return errors.Wrap(err, "stream value decode failed")
						}
						buf = append(buf, *value)
					}
				}
			}

			values := &FixedReStream{Buf: buf}
			if err := n.Out.ProcessElement(ctx, key, values); err != nil {
				return err
			}
		}

	default:
		ec := MakeElementDecoder(c)

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

			elm, err := ec.Decode(r)
			if err != nil {
				return errors.Wrap(err, "source decode failed")
			}
			elm.Timestamp = t
			elm.Windows = ws

			// log.Printf("READ: %v %v", elm.Key.Type(), elm.Key.Interface())

			if err := n.Out.ProcessElement(ctx, elm); err != nil {
				return err
			}
		}
	}
}

func (n *DataSource) FinishBundle(ctx context.Context) error {
	n.mu.Lock()
	log.Infof(ctx, "DataSource: %d elements in %d ns", n.count, time.Now().Sub(n.start))
	n.source = nil
	err := n.Out.FinishBundle(ctx)
	n.count = 0
	n.splitPos = math.MaxInt64
	n.mu.Unlock()
	return err
}

func (n *DataSource) Down(ctx context.Context) error {
	n.source = nil
	return nil
}

func (n *DataSource) String() string {
	return fmt.Sprintf("DataSource[%v] Coder:%v Out:%v", n.SID, n.Coder, n.Out.ID())
}

// IncrementCountAndCheckSplit increments DataSource.count by one and checks to
// make sure the new value is smaller than the promised split point. If the new
// value is greater than or equal to the split point, calls the FinishBundle and
// returns true to indicate that the caller should stop processing elements and
// exit. If the new value is before the split point (or if the split point
// hasn't been set), returns false.
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
	return ProgressReportSnapshot{n.SID.Target.ID, n.SID.Target.Name, c}
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
	// Find the smallest split index that we haven't yet processed.
	p := math.MaxInt64

	for i := int(0); i < len(splits); i++ {
		if splits[i] >= c {
			p = i
			break
		}
	}
	if p < len(splits) {
		n.splitPos = splits[p]
		fs := n.splitPos
		n.mu.Unlock()
		return fs, nil
	}
	n.mu.Unlock()
	// If we can't find a suitable split point from the requested choices,
	// return an error.
	return 0, fmt.Errorf("failed to split at requested splits: {%v}, DataSource at index: %v", splits, c)
}
