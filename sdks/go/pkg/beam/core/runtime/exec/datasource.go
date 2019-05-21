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
	"sync/atomic"
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

	source DataManager
	count  int64
	start  time.Time
}

func (n *DataSource) ID() UnitID {
	return n.UID
}

func (n *DataSource) Up(ctx context.Context) error {
	return nil
}

func (n *DataSource) StartBundle(ctx context.Context, id string, data DataContext) error {
	n.source = data.Data
	n.start = time.Now()
	atomic.StoreInt64(&n.count, 0)
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
				atomic.AddInt64(&n.count, int64(size))

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

					atomic.AddInt64(&n.count, int64(chunk))
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
			atomic.AddInt64(&n.count, 1)
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
	log.Infof(ctx, "DataSource: %d elements in %d ns", atomic.LoadInt64(&n.count), time.Now().Sub(n.start))
	n.source = nil
	err := n.Out.FinishBundle(ctx)
	atomic.StoreInt64(&n.count, 0)
	return err
}

func (n *DataSource) Down(ctx context.Context) error {
	n.source = nil
	return nil
}

func (n *DataSource) String() string {
	return fmt.Sprintf("DataSource[%v] Coder:%v Out:%v", n.SID, n.Coder, n.Out.ID())
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
	return ProgressReportSnapshot{n.SID.Target.ID, n.SID.Target.Name, atomic.LoadInt64(&n.count)}
}
