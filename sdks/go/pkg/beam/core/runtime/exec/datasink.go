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
	"sync/atomic"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

// DataSink is a Node that writes element data to the data service..
type DataSink struct {
	UID   UnitID
	SID   StreamID
	Coder *coder.Coder
	PCol  *PCollection // Handles size metrics.

	enc  ElementEncoder
	wEnc WindowEncoder
	w    io.WriteCloser
}

// ID returns the debug ID.
func (n *DataSink) ID() UnitID {
	return n.UID
}

// Up initializes the element and window encoders.
func (n *DataSink) Up(ctx context.Context) error {
	n.enc = MakeElementEncoder(coder.SkipW(n.Coder))
	n.wEnc = MakeWindowEncoder(n.Coder.Window)
	return nil
}

// StartBundle opens the writer to the data service.
func (n *DataSink) StartBundle(ctx context.Context, id string, data DataContext) error {
	w, err := data.Data.OpenWrite(ctx, n.SID)
	if err != nil {
		return err
	}
	n.w = w
	// TODO[BEAM-6374): Properly handle the multiplex and flatten cases.
	// Right now we just stop datasink collection.
	if n.PCol != nil {
		atomic.StoreInt64(&n.PCol.bundleElementCount, 0)
		n.PCol.resetSize()
	}
	return nil
}

// ProcessElement encodes the windowed value header for the element, followed by the element,
// emitting it to the data service.
func (n *DataSink) ProcessElement(ctx context.Context, value *FullValue, values ...ReStream) error {
	// Marshal the pieces into a temporary buffer since they must be transmitted on FnAPI as a single
	// unit.
	var b bytes.Buffer

	if err := EncodeWindowedValueHeader(n.wEnc, value.Windows, value.Timestamp, value.Pane, &b); err != nil {
		return err
	}
	if err := n.enc.Encode(value, &b); err != nil {
		return errors.WithContextf(err, "encoding element %v with coder %v", value, n.Coder)
	}
	byteCount, err := n.w.Write(b.Bytes())
	if err != nil {
		return err
	}
	// TODO[BEAM-6374): Properly handle the multiplex and flatten cases.
	// Right now we just stop datasink collection.
	if n.PCol != nil {
		atomic.AddInt64(&n.PCol.bundleElementCount, 1)
		n.PCol.addSize(int64(byteCount))
	}
	return nil
}

// FinishBundle closes the write to the data channel.
func (n *DataSink) FinishBundle(ctx context.Context) error {
	return n.w.Close()
}

// Down is a no-op.
func (n *DataSink) Down(ctx context.Context) error {
	return nil
}

func (n *DataSink) String() string {
	return fmt.Sprintf("DataSink[%v] Coder:%v", n.SID, n.Coder)
}
